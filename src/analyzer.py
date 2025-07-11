# src/analyzer.py
import time
import os
import re
import logging
import pandas as pd
from .client import ElasticsearchClient
from .config import SNAPSHOT_DIR, SNAPSHOT_INTERVAL_S, SNAPSHOT_RETENTION_DAYS

class ClusterAnalyzer:
    """Orquesta la recolección, análisis y visualización de datos del clúster."""
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
        self.last_snapshot_time = 0
        self.top_heap_indices = pd.DataFrame()

    def _manage_snapshots(self, current_time):
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        if (current_time - self.last_snapshot_time) > SNAPSHOT_INTERVAL_S:
            timestamp_str = int(current_time)
            if not self.nodes_df.empty:
                self.nodes_df.to_json(f"{SNAPSHOT_DIR}/nodes_{timestamp_str}.json", orient='split')
            if not self.indices_df.empty:
                self.indices_df.to_json(f"{SNAPSHOT_DIR}/indices_{timestamp_str}.json", orient='split')
            self.last_snapshot_time = current_time
            logging.info(f"Snapshot guardado en t={timestamp_str}")

        retention_limit = current_time - (SNAPSHOT_RETENTION_DAYS * 24 * 60 * 60)
        for filename in os.listdir(SNAPSHOT_DIR):
            try:
                timestamp = int(re.search(r'_(\d+)\.json', filename).group(1))
                if timestamp < retention_limit:
                    os.remove(os.path.join(SNAPSHOT_DIR, filename))
                    logging.info(f"Snapshot antiguo purgado: {filename}")
            except (AttributeError, ValueError):
                continue

    def fetch_all_data(self, for_deep_dive=False):
        current_time = time.time()
        self.last_fetch_time = current_time

        if not self.nodes_df.empty:
            self.previous_nodes_df = self.nodes_df.copy()
        if not self.indices_df.empty:
            self.previous_indices_df = self.indices_df.copy()
        if self.node_stats_raw:
            self.previous_node_stats_raw = self.node_stats_raw.copy()

        self.node_stats_raw = self.client.get("_nodes/stats/jvm,fs,os,process,thread_pool,transport,breaker") or {}
        nodes_info = self.client.get("_nodes/_all/info/name,roles,attributes") or {}
        
        if not for_deep_dive:
            index_stats_raw = self.client.get("_stats/indexing,search,segments,query_cache,fielddata") or {}
            cat_indices_raw = self.client.get("_cat/indices?format=json&bytes=mb&h=health,status,index,uuid,pri,rep,docs.count,store.size") or []
            self.shards_df = pd.DataFrame(self.client.get("_cat/shards?format=json&bytes=mb&h=index,shard,prirep,state,docs,store,ip,node") or [])
            self.cluster_stats = self.client.get("_cluster/stats") or {}
            self.cluster_health = self.client.get("_cluster/health") or {}
            self.pending_tasks = self.client.get("_cluster/pending_tasks") or {}
            
            cat_df = pd.DataFrame([i for i in cat_indices_raw if i.get('status') == 'open'])
            stats_list = []
            if 'indices' in index_stats_raw:
                for index_name, stats in index_stats_raw.get('indices', {}).items():
                    stats_list.append({
                        'index': index_name,
                        'indexing_total': stats.get('total', {}).get('indexing', {}).get('index_total', 0),
                        'search_total': stats.get('total', {}).get('search', {}).get('query_total', 0),
                        'segments_count': stats.get('total', {}).get('segments', {}).get('count', 0),
                        'memory_segments_mb': stats.get('total', {}).get('segments', {}).get('memory_in_bytes', 0) / 1e6,
                        'memory_cache_mb': stats.get('total', {}).get('query_cache', {}).get('memory_size_in_bytes', 0) / 1e6,
                        'memory_fielddata_mb': stats.get('total', {}).get('fielddata', {}).get('memory_size_in_bytes', 0) / 1e6,
                    })
            stats_df = pd.DataFrame(stats_list)

            if not cat_df.empty and not stats_df.empty:
                self.indices_df = pd.merge(cat_df, stats_df, on='index', how='inner')
                self.indices_df['heap_usage_mb'] = self.indices_df['memory_segments_mb'] + self.indices_df['memory_cache_mb'] + self.indices_df['memory_fielddata_mb']
                self.top_heap_indices = self.indices_df.sort_values('heap_usage_mb', ascending=False).head(5)
            else:
                self.indices_df = pd.DataFrame() # Ensure it is an empty DataFrame
                self.top_heap_indices = pd.DataFrame()

        node_list = []
        if 'nodes' in self.node_stats_raw:
            for node_id, data in self.node_stats_raw.get('nodes', {}).items():
                jvm_mem = data.get('jvm', {}).get('mem', {})
                old_gen = jvm_mem.get('pools', {}).get('old', {})
                heap_old_gen_percent = (old_gen.get('used_in_bytes', 0) / old_gen.get('max_in_bytes', 1) * 100)
                gc_info = data.get('jvm', {}).get('gc', {}).get('collectors', {}).get('old', {})
                
                node_info = nodes_info.get('nodes', {}).get(node_id, {})
                node_attributes = node_info.get('attributes', {})
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
        
        if not for_deep_dive:
            self._manage_snapshots(current_time)