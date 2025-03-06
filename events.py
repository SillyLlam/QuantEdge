"""Event manager for handling communication between components."""

class EventManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EventManager, cls).__new__(cls)
            cls._instance.listeners = {}
        return cls._instance
    
    def subscribe(self, event_type, callback):
        """Subscribe to an event type."""
        if event_type not in self.listeners:
            self.listeners[event_type] = []
        self.listeners[event_type].append(callback)
    
    def emit(self, event_type, data=None):
        """Emit an event to all subscribers."""
        if event_type in self.listeners:
            for callback in self.listeners[event_type]:
                try:
                    callback(data)
                except Exception as e:
                    print(f"Error in event callback: {str(e)}")

# Global event manager instance
event_manager = EventManager()
