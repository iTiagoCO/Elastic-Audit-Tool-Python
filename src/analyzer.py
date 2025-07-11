# src/analyzer.py
import time
import pandas as pd

from .client import ElasticsearchClient

class ClusterAnalyzer:
    """Orquesta la recolección y el procesamiento de datos del clúster."""
    def __init__(self, client: ElasticsearchClient):
        self.client = client
        self.nodes_df = pd.DataFrame()
        self.indices_df = pd.DataFrame()
        self.shards_df = pd.DataFrame()
        self.previous_nodes_df = pd.DataFrame()
        self.previous_indices_df = pd.DataFrame()
        self.node_stats_raw = {}
        self.previous_node_stats_raw = {}
        self.cluster_stats = {}
        self.cluster_health = {}
        self.pending_tasks = {}
        self.last_fetch_time = None
        self.top_heap_indices = pd.DataFrame()

    def fetch_all_data(self, for_deep_dive=False):
        """Obtiene todos los datos necesarios del clúster usando filter_path para optimizar."""
        self.last_fetch_time = time.time()

        if not self.nodes_df.empty:
            self.previous_nodes_df = self.nodes_df.copy()
        if not self.indices_df.empty:
            self.previous_indices_df = self.indices_df.copy()
        if self.node_stats_raw:
            self.previous_node_stats_raw = self.node_stats_raw.copy()

        # --- Definición de Filtros para las APIs ---
        node_stats_filter = [
            "nodes.*.name", "nodes.*.host", "nodes.*.ip", "nodes.*.roles", "nodes.*.attributes",
            "nodes.*.jvm.mem.heap_used_percent", "nodes.*.jvm.mem.pools.old.used_in_bytes",
            "nodes.*.jvm.mem.pools.old.max_in_bytes", "nodes.*.jvm.gc.collectors.old.collection_count",
            "nodes.*.jvm.gc.collectors.old.collection_time_in_millis",
            "nodes.*.os.cpu.percent", "nodes.*.thread_pool.*.rejected", "nodes.*.breaker.*.tripped"
        ]
        nodes_info_filter = ["nodes.*.name", "nodes.*.roles", "nodes.*.attributes"]
        
        # --- Peticiones Optimizadas ---
        self.node_stats_raw = self.client.get("_nodes/stats/jvm,os,thread_pool,breaker", filter_path=node_stats_filter) or {}
        nodes_info = self.client.get("_nodes/_all/info/name,roles,attributes", filter_path=nodes_info_filter) or {}
        
        if not for_deep_dive:
            index_stats_filter = [
                "indices.*.total.indexing.index_total", "indices.*.total.search.query_total",
                "indices.*.total.segments.count", "indices.*.total.segments.memory_in_bytes",
                "indices.*.total.query_cache.memory_size_in_bytes", "indices.*.total.fielddata.memory_size_in_bytes"
            ]
            cat_indices_raw = self.client.get("_cat/indices", params={'format': 'json', 'bytes': 'mb', 'h': 'health,status,index,uuid,pri,rep,docs.count,store.size'}) or []
            
            self.shards_df = pd.DataFrame(self.client.get("_cat/shards", params={'format': 'json', 'bytes': 'mb', 'h': 'index,shard,prirep,state,docs,store,ip,node'}) or [])
            self.cluster_stats = self.client.get("_cluster/stats", filter_path=["cluster_name", "nodes.jvm.mem.heap_used_in_bytes", "nodes.jvm.mem.heap_max_in_bytes"]) or {}
            self.cluster_health = self.client.get("_cluster/health") or {}
            self.pending_tasks = self.client.get("_cluster/pending_tasks") or {}
            index_stats_raw = self.client.get("_stats/indexing,search,segments,query_cache,fielddata", filter_path=index_stats_filter) or {}
            
            cat_df = pd.DataFrame([i for i in cat_indices_raw if i.get('status') == 'open'])
            stats_list = []
            if 'indices' in index_stats_raw:
                for index_name, stats in index_stats_raw.get('indices', {}).items():
                    total_stats = stats.get('total', {})
                    stats_list.append({
                        'index': index_name,
                        'indexing_total': total_stats.get('indexing', {}).get('index_total', 0),
                        'search_total': total_stats.get('search', {}).get('query_total', 0),
                        'segments_count': total_stats.get('segments', {}).get('count', 0),
                        'memory_segments_mb': total_stats.get('segments', {}).get('memory_in_bytes', 0) / 1e6,
                        'memory_cache_mb': total_stats.get('query_cache', {}).get('memory_size_in_bytes', 0) / 1e6,
                        'memory_fielddata_mb': total_stats.get('fielddata', {}).get('memory_size_in_bytes', 0) / 1e6,
                    })
            stats_df = pd.DataFrame(stats_list)

            if not cat_df.empty and not stats_df.empty:
                self.indices_df = pd.merge(cat_df, stats_df, on='index', how='inner')
                self.indices_df['heap_usage_mb'] = self.indices_df[['memory_segments_mb', 'memory_cache_mb', 'memory_fielddata_mb']].sum(axis=1)
                self.top_heap_indices = self.indices_df.sort_values('heap_usage_mb', ascending=False).head(5)
            else:
                self.indices_df = pd.DataFrame()
                self.top_heap_indices = pd.DataFrame()

        # --- Procesamiento de Datos ---
        node_list = []
        if 'nodes' in self.node_stats_raw:
            for node_id, data in self.node_stats_raw.get('nodes', {}).items():
                jvm_mem = data.get('jvm', {}).get('mem', {})
                old_gen = jvm_mem.get('pools', {}).get('old', {})
                heap_old_gen_percent = (old_gen.get('used_in_bytes', 0) / old_gen.get('max_in_bytes', 1) * 100) if 'max_in_bytes' in old_gen and old_gen['max_in_bytes'] > 0 else 0
                gc_info = data.get('jvm', {}).get('gc', {}).get('collectors', {}).get('old', {})
                
                node_info_data = nodes_info.get('nodes', {}).get(node_id, {})
                node_attributes = node_info_data.get('attributes', {})
                tier = next((v for k, v in node_attributes.items() if 'tier' in k), 'undefined')
                
                rejections = sum(pool.get('rejected', 0) for pool in data.get('thread_pool', {}).values())

                node_list.append({
                    'node_id': node_id,
                    'node_name': data.get('name', 'N/A'),
                    'tier': tier,
                    'cpu_percent': data.get('os', {}).get('cpu', {}).get('percent', 0),
                    'heap_percent': jvm_mem.get('heap_used_percent', 0),
                    'heap_old_gen_percent': heap_old_gen_percent,
                    'gc_count': gc_info.get('collection_count', 0),
                    'gc_time_ms': gc_info.get('collection_time_in_millis', 0),
                    'breakers_tripped': sum(b.get('tripped', 0) for b in data.get('breaker', {}).values()),
                    'rejections': rejections
                })
        self.nodes_df = pd.DataFrame(node_list)