import time
import requests
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict

class LineageMonitor:
    def __init__(self, datahub_url="http://host.docker.internal:8080"):
        self.datahub_url = datahub_url
        self.metrics = defaultdict(list)
        
    def measure_event_latency(self, job_name, run_id):
        """Measure latency between job execution and lineage event arrival"""
        start_time = time.time()
        
        # Poll for lineage events
        max_wait = 30  # 30 seconds max wait
        poll_interval = 0.1  # 100ms polling
        
        while (time.time() - start_time) < max_wait:
            try:
                # Check if lineage event exists
                response = requests.get(f"{self.datahub_url}/api/v1/jobs/{job_name}/runs/{run_id}")
                if response.status_code == 200:
                    event_time = time.time()
                    latency = (event_time - start_time) * 1000  # Convert to milliseconds
                    self.metrics['event_latency'].append(latency)
                    return latency
                    
                time.sleep(poll_interval)
                
            except Exception as e:
                logging.error(f"Error checking lineage event: {e}")
                
        # Event not found within timeout
        self.metrics['event_timeouts'].append(1)
        return None
    
    def measure_event_size(self, job_name, run_id):
        """Measure the size of lineage events"""
        try:
            response = requests.get(f"{self.marquez_url}/api/v1/jobs/{job_name}/runs/{run_id}")
            if response.status_code == 200:
                event_data = response.json()
                event_size = len(json.dumps(event_data).encode('utf-8'))
                self.metrics['event_size'].append(event_size)
                return event_size
        except Exception as e:
            logging.error(f"Error measuring event size: {e}")
        return None
    
    def get_performance_report(self):
        """Generate performance metrics report"""
        if not self.metrics:
            return "No metrics collected"
            
        report = {
            'event_latency': {
                'avg_ms': sum(self.metrics['event_latency']) / len(self.metrics['event_latency']) if self.metrics['event_latency'] else 0,
                'max_ms': max(self.metrics['event_latency']) if self.metrics['event_latency'] else 0,
                'min_ms': min(self.metrics['event_latency']) if self.metrics['event_latency'] else 0,
                'count': len(self.metrics['event_latency'])
            },
            'event_size': {
                'avg_bytes': sum(self.metrics['event_size']) / len(self.metrics['event_size']) if self.metrics['event_size'] else 0,
                'max_bytes': max(self.metrics['event_size']) if self.metrics['event_size'] else 0,
                'min_bytes': min(self.metrics['event_size']) if self.metrics['event_size'] else 0,
                'count': len(self.metrics['event_size'])
            },
            'reliability': {
                'success_rate': (len(self.metrics['event_latency']) / (len(self.metrics['event_latency']) + len(self.metrics['event_timeouts']))) * 100 if (self.metrics['event_latency'] or self.metrics['event_timeouts']) else 0,
                'timeout_count': len(self.metrics['event_timeouts'])
            }
        }
        return report