#!/usr/bin/env python3
"""
WATTx-Monero Merged Mining Proxy

This proxy serves XMRig miners with jobs that can earn rewards on both chains.
When a miner submits a valid share:
- If it meets WATTx difficulty: Submit to WATTx node
- If it meets Monero difficulty: Submit to Monero node
- Miner earns dual rewards when hash meets both targets
"""

import asyncio
import json
import hashlib
import os
import sys
import time
import struct
import binascii
import socket
import threading
from typing import Dict, Optional, Tuple
import requests

# Configuration
CONFIG = {
    # WATTx node
    "wattx_rpc_host": "127.0.0.1",
    "wattx_rpc_port": 3889,
    "wattx_rpc_user": "wattxrpc",
    "wattx_rpc_pass": "wattxpass123",

    # Monero daemon (using public node until local syncs)
    "monero_daemon_host": "xmr-node.cakewallet.com",
    "monero_daemon_port": 18081,

    # Stratum server
    "stratum_host": "0.0.0.0",
    "stratum_port": 3337,

    # Mining addresses
    "wattx_address": "WZzugKM8P9L3Ds2PjqoZUBVvESqtA5RCUr",
    "monero_address": "4AsjKppNcHfJPekAPKVMsecyVT1v35MVn4N6dsXYSVTZHWsmC66u3sDT5NYavm5udMXHf32Ntb4N2bJqhnN4Gfq2GKZYmMK",

    # Pool settings
    "job_refresh_interval": 30,  # seconds
    "share_difficulty": 1000,    # Low diff for testing
}

class MergedMiningProxy:
    def __init__(self):
        self.clients: Dict[int, dict] = {}
        self.current_job: Optional[dict] = None
        self.job_id = 0
        self.running = False
        self.lock = threading.Lock()

        # Statistics
        self.wtx_shares = 0
        self.xmr_shares = 0
        self.wtx_blocks = 0
        self.xmr_blocks = 0

    def wattx_rpc(self, method: str, params: list = None) -> dict:
        """Call WATTx RPC"""
        if params is None:
            params = []

        url = f"http://{CONFIG['wattx_rpc_host']}:{CONFIG['wattx_rpc_port']}"
        payload = {
            "jsonrpc": "1.0",
            "id": "merged_proxy",
            "method": method,
            "params": params
        }

        try:
            auth = (CONFIG['wattx_rpc_user'], CONFIG['wattx_rpc_pass'])
            resp = requests.post(url, json=payload, auth=auth, timeout=10)
            return resp.json()
        except Exception as e:
            print(f"WATTx RPC error: {e}")
            return {"error": str(e)}

    def monero_rpc(self, method: str, params: dict = None) -> dict:
        """Call Monero daemon RPC"""
        if params is None:
            params = {}

        url = f"http://{CONFIG['monero_daemon_host']}:{CONFIG['monero_daemon_port']}/json_rpc"
        payload = {
            "jsonrpc": "2.0",
            "id": "0",
            "method": method,
            "params": params
        }

        try:
            resp = requests.post(url, json=payload, timeout=10)
            return resp.json()
        except Exception as e:
            print(f"Monero RPC error: {e}")
            return {"error": str(e)}

    def get_wattx_block_template(self) -> Optional[dict]:
        """Get block template from WATTx node"""
        result = self.wattx_rpc("getblocktemplate", [{"rules": ["segwit"]}])
        if "result" in result:
            return result["result"]
        print(f"Failed to get WATTx template: {result}")
        return None

    def difficulty_to_target(self, difficulty: int) -> str:
        """Convert difficulty to target hex string (64 chars)"""
        if difficulty == 0:
            return "f" * 64

        # Target = 2^256 / difficulty
        max_target = (1 << 256) - 1
        target = max_target // difficulty
        return f"{target:064x}"

    def create_merged_job(self) -> dict:
        """Create a merged mining job for both chains"""
        self.job_id += 1
        job_id = f"{self.job_id:08x}"

        job = {
            "id": job_id,
            "created_at": time.time(),
        }

        # Get WATTx template
        wtx_template = self.get_wattx_block_template()
        if wtx_template:
            job["wattx"] = {
                "height": wtx_template.get("height", 0),
                "previousblockhash": wtx_template.get("previousblockhash", ""),
                "bits": wtx_template.get("bits", ""),
                "target": wtx_template.get("target", ""),
                "transactions": wtx_template.get("transactions", []),
                "coinbasevalue": wtx_template.get("coinbasevalue", 0),
            }
            print(f"WATTx template: height={job['wattx']['height']}, reward={job['wattx']['coinbasevalue']/1e8:.2f} WTX")

            # Create a mining blob (76 bytes for RandomX)
            # This is a simplified version - real implementation needs proper block header
            blob = bytearray(76)

            # Version (4 bytes)
            struct.pack_into("<I", blob, 0, 4)

            # Previous block hash (32 bytes) - reversed for little endian
            prev_hash = bytes.fromhex(job["wattx"]["previousblockhash"])
            blob[4:36] = prev_hash[::-1]

            # Merkle root placeholder (32 bytes)
            blob[36:68] = hashlib.sha256(b"wattx_merged_mining").digest()

            # Timestamp (4 bytes)
            struct.pack_into("<I", blob, 68, int(time.time()))

            # nBits (4 bytes)
            bits = int(job["wattx"].get("bits", "1f00ffff"), 16)
            struct.pack_into("<I", blob, 72, bits)

            job["blob"] = blob.hex()

            # Get seed hash from a recent block
            seed_height = (job["wattx"]["height"] // 2048) * 2048
            if seed_height == 0:
                job["seed_hash"] = "0" * 64
            else:
                result = self.wattx_rpc("getblockhash", [seed_height])
                if "result" in result:
                    job["seed_hash"] = result["result"]
                else:
                    job["seed_hash"] = "0" * 64

            # Use share difficulty for target
            job["target"] = self.difficulty_to_target(CONFIG["share_difficulty"])[-16:]  # XMRig uses 8 bytes (16 hex chars)

        return job

    def handle_client(self, client_socket: socket.socket, client_id: int):
        """Handle stratum client connection"""
        print(f"Client {client_id} connected from {client_socket.getpeername()}")

        self.clients[client_id] = {
            "socket": client_socket,
            "authorized": False,
            "worker": "",
            "shares": 0,
        }

        buffer = ""

        try:
            while self.running:
                try:
                    data = client_socket.recv(4096)
                    if not data:
                        break

                    buffer += data.decode('utf-8')

                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        if line.strip():
                            print(f"Client {client_id} >>> {line.strip()}")
                            self.handle_message(client_id, line.strip())
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"Client {client_id} recv error: {e}")
                    break
        except Exception as e:
            print(f"Client {client_id} handler error: {e}")
        finally:
            print(f"Client {client_id} disconnected")
            try:
                client_socket.close()
            except:
                pass
            if client_id in self.clients:
                del self.clients[client_id]

    def handle_message(self, client_id: int, message: str):
        """Handle stratum message from client"""
        try:
            msg = json.loads(message)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON from client {client_id}: {message} - {e}")
            return

        method = msg.get("method", "")
        params = msg.get("params", {})
        msg_id = msg.get("id", 0)

        print(f"Client {client_id} method={method}, id={msg_id}")

        if method == "login":
            self.handle_login(client_id, msg_id, params)
        elif method == "submit":
            self.handle_submit(client_id, msg_id, params)
        elif method == "getjob":
            self.handle_getjob(client_id, msg_id)
        elif method == "keepalived":
            self.send_response(client_id, msg_id, {"status": "KEEPALIVED"})
        else:
            print(f"Unknown method from client {client_id}: {method}")
            self.send_error(client_id, msg_id, -1, f"Unknown method: {method}")

    def handle_login(self, client_id: int, msg_id: int, params: dict):
        """Handle miner login"""
        login = params.get("login", "")
        password = params.get("pass", "x")

        # Parse login format: ADDRESS.WORKER
        if "." in login:
            address, worker = login.rsplit(".", 1)
        else:
            address = login
            worker = "default"

        self.clients[client_id]["worker"] = worker
        self.clients[client_id]["address"] = address
        self.clients[client_id]["authorized"] = True

        print(f"Client {client_id} logged in as {worker} (addr: {address[:20]}...)")

        # Create job if needed
        if not self.current_job:
            self.current_job = self.create_merged_job()

        job = self.current_job

        # XMRig login response format
        response = {
            "id": msg_id,
            "jsonrpc": "2.0",
            "result": {
                "id": f"client{client_id}",
                "job": {
                    "blob": job.get("blob", "0" * 152),
                    "job_id": job["id"],
                    "target": job.get("target", "ffffffff"),
                    "seed_hash": job.get("seed_hash", "0" * 64),
                    "height": job.get("wattx", {}).get("height", 0),
                    "algo": "rx/0"
                },
                "status": "OK"
            },
            "error": None
        }

        print(f"Sending login response to client {client_id}")
        self.send_json(client_id, response)

    def handle_submit(self, client_id: int, msg_id: int, params: dict):
        """Handle share submission"""
        job_id = params.get("job_id", "")
        nonce = params.get("nonce", "")
        result_hash = params.get("result", "")

        print(f"Share from client {client_id}: job={job_id}, nonce={nonce}, hash={result_hash[:16]}...")

        # Accept all shares for testing
        self.clients[client_id]["shares"] += 1
        self.wtx_shares += 1

        # Check if it's a valid WATTx block (simplified)
        if result_hash:
            # In real implementation, verify RandomX hash and check against target
            print(f"Share accepted from client {client_id}")

        response = {
            "id": msg_id,
            "jsonrpc": "2.0",
            "result": {"status": "OK"},
            "error": None
        }
        self.send_json(client_id, response)

    def handle_getjob(self, client_id: int, msg_id: int):
        """Send new job to client"""
        if not self.current_job:
            self.current_job = self.create_merged_job()

        job = self.current_job

        # XMRig job notification
        notification = {
            "jsonrpc": "2.0",
            "method": "job",
            "params": {
                "blob": job.get("blob", "0" * 152),
                "job_id": job["id"],
                "target": job.get("target", "ffffffff"),
                "seed_hash": job.get("seed_hash", "0" * 64),
                "height": job.get("wattx", {}).get("height", 0),
                "algo": "rx/0"
            }
        }

        self.send_json(client_id, notification)

    def send_response(self, client_id: int, msg_id: int, result: dict):
        """Send JSON-RPC response"""
        response = {
            "id": msg_id,
            "jsonrpc": "2.0",
            "result": result,
            "error": None
        }
        self.send_json(client_id, response)

    def send_error(self, client_id: int, msg_id: int, code: int, message: str):
        """Send JSON-RPC error"""
        response = {
            "id": msg_id,
            "jsonrpc": "2.0",
            "result": None,
            "error": {"code": code, "message": message}
        }
        self.send_json(client_id, response)

    def send_json(self, client_id: int, data: dict):
        """Send JSON message to client"""
        if client_id in self.clients:
            try:
                message = json.dumps(data) + "\n"
                print(f"Client {client_id} <<< {message.strip()}")
                self.clients[client_id]["socket"].send(message.encode('utf-8'))
            except Exception as e:
                print(f"Error sending to client {client_id}: {e}")

    def broadcast_job(self, job: dict):
        """Broadcast new job to all clients"""
        for client_id in list(self.clients.keys()):
            if self.clients.get(client_id, {}).get("authorized"):
                self.handle_getjob(client_id, 0)

    def job_refresh_loop(self):
        """Periodically refresh mining job"""
        while self.running:
            time.sleep(CONFIG["job_refresh_interval"])

            if self.running:
                print(f"\n=== Refreshing job ===")
                self.current_job = self.create_merged_job()
                print(f"Broadcasting to {len(self.clients)} clients\n")
                self.broadcast_job(self.current_job)

    def run(self):
        """Run the stratum server"""
        self.running = True

        print("=" * 60)
        print("WATTx-Monero Merged Mining Proxy")
        print("=" * 60)

        # Create initial job
        print("\nFetching initial block template...")
        self.current_job = self.create_merged_job()

        if not self.current_job.get("blob"):
            print("ERROR: Failed to get block template from WATTx node!")
            print(f"Make sure wattxd is running on {CONFIG['wattx_rpc_host']}:{CONFIG['wattx_rpc_port']}")
            return

        # Start job refresh thread
        refresh_thread = threading.Thread(target=self.job_refresh_loop, daemon=True)
        refresh_thread.start()

        # Create server socket
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((CONFIG["stratum_host"], CONFIG["stratum_port"]))
        server.listen(100)
        server.settimeout(1.0)

        print(f"\nStratum server listening on {CONFIG['stratum_host']}:{CONFIG['stratum_port']}")
        print(f"WATTx RPC: {CONFIG['wattx_rpc_host']}:{CONFIG['wattx_rpc_port']}")
        print()
        print("Connect XMRig with:")
        print(f'  ./xmrig -o 127.0.0.1:{CONFIG["stratum_port"]} -u {CONFIG["wattx_address"]}.worker -p x -a rx/0')
        print()
        print("Waiting for miners...\n")

        client_id = 0

        try:
            while self.running:
                try:
                    client_socket, addr = server.accept()
                    client_socket.settimeout(60)
                    client_id += 1

                    thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_id),
                        daemon=True
                    )
                    thread.start()
                except socket.timeout:
                    continue
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self.running = False
            server.close()

        print(f"\nStatistics:")
        print(f"  WATTx shares: {self.wtx_shares}")
        print(f"  Monero shares: {self.xmr_shares}")
        print(f"  WATTx blocks: {self.wtx_blocks}")
        print(f"  Monero blocks: {self.xmr_blocks}")


if __name__ == "__main__":
    proxy = MergedMiningProxy()
    proxy.run()
