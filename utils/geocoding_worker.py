#!/usr/bin/env python3
"""
Background Geocoding Worker
Processes geocoding requests asynchronously without blocking sensor data ingestion
"""

import asyncio
import threading
from queue import Queue
from typing import Tuple, Optional
from .geocoder import get_geocoder

class GeocodingWorker:
    def __init__(self):
        self.queue = Queue()
        self.geocoder = get_geocoder()
        self.running = False
        self.thread = None
        self.loop = None
    
    def start(self):
        """Start the background geocoding worker"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_worker, daemon=True)
        self.thread.start()
        print("✓ Geocoding worker started")
    
    def _run_worker(self):
        """Worker thread main loop"""
        # Create new event loop for this thread
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            self.loop.run_until_complete(self._process_queue())
        finally:
            self.loop.close()
    
    async def _process_queue(self):
        """Process geocoding requests from queue"""
        while self.running:
            try:
                # Check queue with timeout
                if not self.queue.empty():
                    lat, lon, callback = self.queue.get(timeout=0.1)
                    
                    # Check cache first (fast, no API call)
                    location_data = self.geocoder.get_cached_location(lat, lon)
                    
                    if location_data is None:
                        # Not in cache, do API call
                        location_data = await self.geocoder.reverse_geocode(lat, lon)
                    
                    # Call callback with result if provided
                    if callback and location_data:
                        try:
                            callback(lat, lon, location_data)
                        except Exception as e:
                            print(f"Error in geocoding callback: {e}")
                    
                    self.queue.task_done()
                else:
                    # Queue empty, sleep briefly
                    await asyncio.sleep(0.1)
            
            except Exception as e:
                print(f"Geocoding worker error: {e}")
                await asyncio.sleep(1)
    
    def geocode_async(self, lat: float, lon: float, callback=None):
        """
        Queue a geocoding request (non-blocking)
        
        Args:
            lat: Latitude
            lon: Longitude
            callback: Optional function(lat, lon, location_data) to call with result
        """
        # Check cache synchronously first (instant)
        cached = self.geocoder.get_cached_location(lat, lon)
        if cached and callback:
            # Already have it, call callback immediately
            callback(lat, lon, cached)
            return
        
        # Not in cache, queue for async processing
        self.queue.put((lat, lon, callback))
    
    def stop(self):
        """Stop the geocoding worker"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        print("✓ Geocoding worker stopped")


# Global worker instance
_worker_instance = None

def get_worker() -> GeocodingWorker:
    """Get or create global geocoding worker"""
    global _worker_instance
    if _worker_instance is None:
        _worker_instance = GeocodingWorker()
        _worker_instance.start()
    return _worker_instance
