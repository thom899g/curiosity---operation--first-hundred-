#!/usr/bin/env python3
"""
Base Mempool Instrumentation - Sensing Layer v1

Monitors Base blockchain mempool for:
1. Large pending swaps (>1 ETH)
2. Contract creations
3. MEV transaction patterns
4. Gas price anomalies

Architectural Choice: Uses WebSocket for real-time mempool streaming
rather than polling to minimize RPC calls and capture true pending state.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from web3 import Web3