import tensorflow as tf
import numpy as np
from typing import Dict, List
import re

class SensitivityDetector:
    def __init__(self):
        self.model = self._build_model()
        self._load_pretrained_weights()
        self.sensitive_patterns = {
            'email': r'^[\w\.-]+@[\w\.-]+\.\w+$',
            'phone': r'^\+?1?\d{9,15}$',
            'ssn': r'^\d{3}-?\d{2}-?\d{4}$',
            'credit_card': r'^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}$',
            'address': r'\d+\s+[\w\s]+(?:street|st|avenue|ave|road|rd|boulevard|blvd)\.?',
        }

    def _build_model(self):
        """Build a lightweight neural network for sensitivity detection"""
        model = tf.keras.Sequential([
            tf.keras.layers.Input(shape=(100,)),
            tf.keras.layers.Dense(64, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(32, activation='relu'),
            tf.keras.layers.Dense(16, activation='relu'),
            tf.keras.layers.Dense(1, activation='sigmoid')
        ])
        model.compile(optimizer='adam',
                     loss='binary_crossentropy',
                     metrics=['accuracy'])
        return model

    def _load_pretrained_weights(self):
        """In production, load pretrained weights"""
        # This would load pretrained weights
        pass

    def _preprocess_text(self, text: str) -> np.ndarray:
        """Convert text to numerical features"""
        # Simple preprocessing - convert to lowercase and pad/truncate
        text = str(text).lower()
        # Pad or truncate to 100 characters
        if len(text) > 100:
            text = text[:100]
        else:
            text = text.ljust(100)
        
        # Convert to numerical values (simple ASCII values for demo)
        return np.array([ord(c) for c in text]) / 255.0

    def _pattern_match(self, text: str) -> bool:
        """Check if text matches any sensitive patterns"""
        for pattern in self.sensitive_patterns.values():
            if re.match(pattern, str(text)):
                return True
        return False

    def detect(self, data: Dict) -> List[str]:
        """Detect sensitive fields in the data"""
        sensitive_fields = []

        for field, value in data.items():
            # Skip None values
            if value is None:
                continue

            # Convert value to string for processing
            value_str = str(value)

            # Check pattern matching first (faster)
            if self._pattern_match(value_str):
                sensitive_fields.append(field)
                continue

            # If no pattern match, use AI model
            features = self._preprocess_text(value_str)
            prediction = self.model.predict(features.reshape(1, -1), verbose=0)[0][0]
            
            if prediction > 0.5:  # Threshold for sensitivity
                sensitive_fields.append(field)

        return sensitive_fields

    def train(self, training_data: List[Dict], labels: List[bool]):
        """Train the model on new data"""
        # Preprocess training data
        X = np.array([self._preprocess_text(str(d)) for d in training_data])
        y = np.array(labels)

        # Train the model
        self.model.fit(X, y, epochs=5, batch_size=32, validation_split=0.2)

    def save_model(self, path: str):
        """Save the trained model"""
        self.model.save(path)
