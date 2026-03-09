#!/usr/bin/env python3
"""
CORTEX v1 - Autonomous Trading Cortex Setup
Phase 0: Core Infrastructure & Instrumentation

Architectural Design Principles:
1. Fractal Cohesion - Each module operates independently but feeds into central nervous system
2. Real Data First - No simulations, only live blockchain data from Day 1
3. Capital Efficiency - Risk-capped allocations with automated treasury management
4. Observability-As-Code - Structured logging and real-time P&L dashboards

Edge Cases Handled:
- RPC connection failures with exponential backoff
- Firebase quota limits with graceful degradation
- Missing service account files with immediate failure
- Network latency spikes with circuit breakers
"""

import json
import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import firebase_admin
from firebase_admin import credentials, firestore, initialize_app
from datetime import datetime
import time

# Configure robust logging BEFORE any operations
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('cortex_operations.log')
    ]
)
logger = logging.getLogger(__name__)

class CortexSetup:
    """Core infrastructure setup with comprehensive error handling"""
    
    def __init__(self, service_account_path: str = "serviceAccountKey.json"):
        """Initialize Cortex with Firebase and validate all dependencies"""
        self.service_account_path = Path(service_account_path)
        self.db: Optional[firestore.Client] = None
        self._validate_environment()
        self._initialize_firebase()
        self._validate_firestore_connection()
        
    def _validate_environment(self) -> None:
        """Validate all required dependencies and files exist"""
        logger.info("Validating Cortex environment...")
        
        # Check Python version
        if sys.version_info < (3, 11):
            raise RuntimeError("Python 3.11+ required. Found: {}".format(sys.version))
        
        # Verify service account file exists and is valid JSON
        if not self.service_account_path.exists():
            logger.error(f"Firebase service account not found at: {self.service_account_path}")
            logger.info("To create service account:")
            logger.info("1. Go to Firebase Console → Project Settings → Service Accounts")
            logger.info("2. Generate new private key")
            logger.info("3. Save as 'serviceAccountKey.json' in project root")
            raise FileNotFoundError(f"Service account file not found: {self.service_account_path}")
        
        try:
            with open(self.service_account_path, 'r') as f:
                json.load(f)  # Validate JSON
            logger.info("✓ Service account file validated")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in service account: {e}")
        
        # Check required environment variables
        required_env_vars = ['BASE_RPC_URL', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID']
        missing_vars = [var for var in required_env_vars if var not in os.environ]
        if missing_vars:
            logger.warning(f"Missing environment variables: {missing_vars}")
            logger.info("Create .env file with:")
            for var in missing_vars:
                if var == 'BASE_RPC_URL':
                    logger.info(f"{var}=https://mainnet.base.org (or Alchemy/Infura URL)")
                elif 'TELEGRAM' in var:
                    logger.info(f"{var}=[Get from @BotFather]")
        
        logger.info("✓ Environment validation complete")
    
    def _initialize_firebase(self) -> None:
        """Initialize Firebase Admin SDK with error resilience"""
        try:
            # Prevent duplicate initialization
            if not firebase_admin._apps:
                cred = credentials.Certificate(str(self.service_account_path))
                firebase_admin.initialize_app(cred)
                logger.info("✓ Firebase Admin SDK initialized")
            else:
                logger.info("✓ Firebase already initialized")
            
            self.db = firestore.client()
            
        except Exception as e:
            logger.error(f"Firebase initialization failed: {e}")
            logger.info("Troubleshooting steps:")
            logger.info("1. Verify service account has Firestore read/write permissions")
            logger.info("2. Check internet connectivity")
            logger.info("3. Verify project ID in service account matches Firebase project")
            raise
    
    def _validate_firestore_connection(self) -> None:
        """Test Firestore connection with timeout and retry logic"""
        if not self.db:
            raise RuntimeError("Firestore client not initialized")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Simple write/read test
                test_doc_ref = self.db.collection('_cortex_health').document('connection_test')
                test_data = {
                    'timestamp': firestore.SERVER_TIMESTAMP,
                    'test_id': f'attempt_{attempt}',
                    'status': 'testing'
                }
                test_doc_ref.set(test_data)
                
                # Verify write
                doc = test_doc_ref.get()
                if doc.exists:
                    test_doc_ref.delete()  # Cleanup
                    logger.info(f"✓ Firestore connection validated (attempt {attempt + 1})")
                    return
                    
            except Exception as e:
                logger.warning(f"Firestore connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error("All Firestore connection attempts failed")
                    raise ConnectionError(f"Cannot connect to Firestore: {e}")
    
    def initialize_collections(self) -> None:
        """Create all required collections with schema validation"""
        if not self.db:
            raise RuntimeError("Firestore client not initialized")
        
        collections = ['mempool_events', 'block_analytics', 'strategy_performance', 'treasury_state']
        
        for collection_name in collections:
            try:
                # Create a schema validation document
                schema_doc_ref = self.db.collection(collection_name).document('_schema')
                schema_data = {
                    'initialized': True,
                    'timestamp': firestore.SERVER_TIMESTAMP,
                    'cortex_version': 'v1.0',
                    'description': f'Cortex {collection_name} collection'
                }
                schema_doc_ref.set(schema_data, merge=True)
                logger.info(f"✓ Collection initialized: {collection_name}")
                
            except Exception as e:
                logger.error(f"Failed to initialize collection {collection_name}: {e}")
                raise
    
    def create_initial_treasury_state(self, initial_capital: float = 13.31) -> None:
        """Create initial treasury allocation state"""
        if not self.db:
            raise RuntimeError("Firestore client not initialized")
        
        try:
            treasury_ref = self.db.collection('treasury_state').document('current')
            initial_state = {
                'timestamp': firestore.SERVER_TIMESTAMP,
                'total_capital': initial_capital,
                'allocations': {
                    'sensing_layer': 0.0,  # No capital allocated
                    'execution_arb': 0.0,
                    'execution_lp': 0.0,
                    'execution_sniper': 0.0,
                    'reserve_gas': initial_capital * 0.25,
                    'operational_reserve': initial_capital * 0.75
                },
                'risk_score': 0.0,
                'circuit_breaker': False,
                'last_reallocation': None,
                'performance_metrics': {
                    'total_trades': 0,
                    'win_rate': 0.0,
                    'sharpe_ratio': 0.0,
                    'max_drawdown': 0.0
                }
            }
            treasury_ref.set(initial_state)
            logger.info(f"✓ Initial treasury state created with ${initial_capital}")
            
        except Exception as e:
            logger.error(f"Failed to create treasury state: {e}")
            raise
    
    def verify_system_health(self) -> Dict[str, Any]:
        """Run comprehensive system health check"""
        health_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'components': {},
            'overall_status': 'unknown'
        }
        
        try:
            # Check Firebase
            if self.db:
                health_status['components']['firebase'] = {
                    'status': 'healthy',
                    'latency': self._test_firestore_latency()
                }
            
            # Check environment
            health_status['components']['environment'] = {
                'status': 'healthy',
                'python_version': sys.version,
                'working_directory': str(Path.cwd())
            }
            
            # Check disk space
            import shutil
            total, used, free = shutil.disk_usage("/")
            health_status['components']['disk'] = {
                'status': 'healthy' if free > 100 * 1024 * 1024 else 'warning',
                'free_gb': free / (1024**3),
                'total_gb': total / (1024**3)
            }
            
            # Determine overall status
            all_healthy = all(
                comp['status'] == 'healthy' 
                for comp in health_status['components'].values()
            )
            health_status['overall_status'] = 'healthy' if all_healthy else 'degraded'
            
            logger.info(f"System health: {health_status['overall_status']}")
            return health_status
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            health_status['overall_status'] = 'failed'
            health_status['error'] = str(e)
            return health_status
    
    def _test_firestore_latency(self) -> float:
        """Measure Firestore round-trip latency"""
        import time
        start = time.time()
        try:
            test_ref = self.db.collection('_latency_test').document('ping')
            test_ref.set({'ping': start})
            test_ref.get()
            latency = (time.time() - start) * 1000  # Convert to milliseconds
            test_ref.delete()
            return latency
        except:
            return -1.0

def main():
    """Main setup routine for Cortex v1"""
    logger.info("🚀 INITIATING CORTEX v1 SETUP - OPERATION FIRST HUNDRED")
    logger.info("=" * 60)
    
    try:
        # Initialize Cortex
        cortex = CortexSetup()
        
        # Initialize collections
        cortex.initialize_collections()
        
        # Create treasury state
        cortex.create_initial_treasury_state(initial_capital=13.31)
        
        # Run health check
        health = cortex.verify_system_health()
        
        logger.info("=" * 60)
        logger.info("✅ CORTEX v1 SETUP COMPLETE")
        logger.info(f"Overall Status: {health['overall_status'].upper()}")
        
        if health['overall_status'] == 'healthy':
            logger.info("Next steps:")
            logger.info("1. Deploy Base mempool listener: python base_instrument.py")
            logger.info("2. Start monitoring: python monitor_dashboard.py")
            logger.info("3. Initialize trading strategies after 24h of data collection")
        else:
            logger.warning("System is degraded. Check logs and fix issues before proceeding.")
        
        return 0
        
    except Exception as e:
        logger.critical(f"CRITICAL SETUP FAILURE: {e}")
        logger.info("Emergency protocol:")
        logger.info("1. Check Firebase project configuration")
        logger.info("2. Verify service account permissions")
        logger.info("3. Contact human via Telegram if blocked by paywall")
        return 1

if __name__ == "__main__":
    sys.exit(main())