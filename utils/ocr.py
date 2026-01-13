"""OCR utilities for text extraction from images and PDFs."""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class OCREngine(ABC):
    """Abstract base class for OCR engines."""
    
    @abstractmethod
    async def extract_text(self, image_path: str) -> Dict[str, any]:
        """Extract text from image.
        
        Args:
            image_path: Path to image file
            
        Returns:
            Dictionary with 'text' and 'confidence' keys
        """
        pass


class PaddleOCREngine(OCREngine):
    """PaddleOCR implementation."""
    
    def __init__(self):
        """Initialize PaddleOCR engine."""
        self.ocr = None
        logger.info("PaddleOCREngine initialized (lazy loading)")
    
    def _ensure_loaded(self):
        """Lazy load PaddleOCR."""
        if self.ocr is None:
            try:
                from paddleocr import PaddleOCR
                self.ocr = PaddleOCR(use_angle_cls=True, lang='ch')
                logger.info("PaddleOCR loaded successfully")
            except ImportError:
                logger.error("PaddleOCR not installed. Install with: pip install paddleocr")
                raise
    
    async def extract_text(self, image_path: str) -> Dict[str, any]:
        """Extract text using PaddleOCR.
        
        Args:
            image_path: Path to image file
            
        Returns:
            Dictionary with extracted text and confidence
        """
        self._ensure_loaded()
        
        try:
            result = self.ocr.ocr(image_path, cls=True)
            
            if not result or not result[0]:
                return {'text': '', 'confidence': 0.0}
            
            # Extract text and confidence
            texts = []
            confidences = []
            
            for line in result[0]:
                text = line[1][0]
                confidence = line[1][1]
                texts.append(text)
                confidences.append(confidence)
            
            full_text = '\n'.join(texts)
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            return {
                'text': full_text,
                'confidence': avg_confidence
            }
            
        except Exception as e:
            logger.error(f"PaddleOCR extraction failed: {e}")
            return {'text': '', 'confidence': 0.0}


class TesseractEngine(OCREngine):
    """Tesseract OCR implementation."""
    
    def __init__(self):
        """Initialize Tesseract engine."""
        logger.info("TesseractEngine initialized")
    
    async def extract_text(self, image_path: str) -> Dict[str, any]:
        """Extract text using Tesseract.
        
        Args:
            image_path: Path to image file
            
        Returns:
            Dictionary with extracted text and confidence
        """
        try:
            import pytesseract
            from PIL import Image
            
            image = Image.open(image_path)
            text = pytesseract.image_to_string(image, lang='chi_sim+eng')
            
            # Tesseract doesn't provide confidence easily, use 0.8 as default
            return {
                'text': text,
                'confidence': 0.8
            }
            
        except ImportError:
            logger.error("Tesseract not installed. Install with: pip install pytesseract")
            return {'text': '', 'confidence': 0.0}
        except Exception as e:
            logger.error(f"Tesseract extraction failed: {e}")
            return {'text': '', 'confidence': 0.0}


def preprocess_image(image_path: str, output_path: Optional[str] = None) -> str:
    """Preprocess image for better OCR results.
    
    Args:
        image_path: Path to input image
        output_path: Path to save preprocessed image (optional)
        
    Returns:
        Path to preprocessed image
    """
    try:
        from PIL import Image, ImageEnhance
        import cv2
        import numpy as np
        
        # Read image
        img = cv2.imread(image_path)
        
        # Convert to grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Apply thresholding
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # Denoise
        denoised = cv2.fastNlMeansDenoising(thresh)
        
        # Save
        if output_path is None:
            output_path = str(Path(image_path).with_suffix('.preprocessed.png'))
        
        cv2.imwrite(output_path, denoised)
        logger.info(f"Preprocessed image saved to: {output_path}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Image preprocessing failed: {e}")
        return image_path  # Return original if preprocessing fails


def get_ocr_engine(engine_type: str = 'paddle') -> OCREngine:
    """Factory function to get OCR engine.
    
    Args:
        engine_type: 'paddle' or 'tesseract'
        
    Returns:
        OCR engine instance
    """
    if engine_type == 'paddle':
        try:
            return PaddleOCREngine()
        except Exception as e:
            logger.warning(f"Failed to load PaddleOCR, falling back to Tesseract: {e}")
            return TesseractEngine()
    elif engine_type == 'tesseract':
        return TesseractEngine()
    else:
        raise ValueError(f"Unknown OCR engine type: {engine_type}")


__all__ = [
    'OCREngine',
    'PaddleOCREngine',
    'TesseractEngine',
    'preprocess_image',
    'get_ocr_engine'
]
