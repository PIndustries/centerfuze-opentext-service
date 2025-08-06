"""
NATS controller for the CenterFuze OpenText Service.

This module handles NATS message routing and processing for OpenText operations.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import asdict

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from app.services.opentext_service import OpenTextService
from app.models.opentext import (
    OpenTextAccount, FaxUsage, NumberPorting, UsageData, UsageAggregation,
    AccountStatus, PortingStatus, UsageDataType
)


logger = logging.getLogger(__name__)


class NATSController:
    """
    NATS message controller for handling OpenText service operations.
    
    This controller manages NATS topic subscriptions and message processing for:
    - Account synchronization and retrieval
    - Fax usage tracking and reporting
    - Number porting status management
    - Usage data aggregation
    """

    def __init__(self, nats_client: NATS, opentext_service: OpenTextService):
        """
        Initialize the NATS controller.
        
        Args:
            nats_client: NATS client instance
            opentext_service: OpenText service instance
        """
        self.nats = nats_client
        self.opentext_service = opentext_service
        self._subscriptions = []

    async def setup_subscriptions(self) -> None:
        """Set up NATS topic subscriptions."""
        logger.info("Setting up NATS subscriptions")
        
        # Account management topics
        await self._subscribe("opentext.account.sync", self._handle_account_sync)
        await self._subscribe("opentext.account.get", self._handle_account_get)
        
        # Fax usage topics
        await self._subscribe("opentext.fax.usage.get", self._handle_fax_usage_get)
        await self._subscribe("opentext.fax.usage.sync", self._handle_fax_usage_sync)
        
        # Number porting topics
        await self._subscribe("opentext.porting.status", self._handle_porting_status)
        await self._subscribe("opentext.porting.update", self._handle_porting_update)
        
        # Usage aggregation topic
        await self._subscribe("opentext.usage.aggregate", self._handle_usage_aggregate)
        
        # Health check topic
        await self._subscribe("opentext.health.check", self._handle_health_check)
        
        logger.info(f"Set up {len(self._subscriptions)} NATS subscriptions")

    async def _subscribe(self, subject: str, handler) -> None:
        """
        Subscribe to a NATS subject with error handling.
        
        Args:
            subject: NATS subject to subscribe to
            handler: Message handler function
        """
        try:
            subscription = await self.nats.subscribe(subject, cb=handler)
            self._subscriptions.append(subscription)
            logger.info(f"Subscribed to {subject}")
        except Exception as e:
            logger.error(f"Failed to subscribe to {subject}: {e}")

    async def _send_response(
        self,
        msg: Msg,
        response_data: Dict[str, Any],
        success: bool = True
    ) -> None:
        """
        Send a response message.
        
        Args:
            msg: Original NATS message
            response_data: Response data
            success: Whether the operation was successful
        """
        response = {
            "success": success,
            "timestamp": datetime.now().isoformat(),
            "data": response_data
        }
        
        if msg.reply:
            try:
                await self.nats.publish(msg.reply, json.dumps(response).encode())
            except Exception as e:
                logger.error(f"Failed to send response: {e}")

    async def _send_error_response(self, msg: Msg, error_message: str) -> None:
        """
        Send an error response message.
        
        Args:
            msg: Original NATS message
            error_message: Error message
        """
        await self._send_response(
            msg,
            {"error": error_message},
            success=False
        )

    # Account Management Handlers

    async def _handle_account_sync(self, msg: Msg) -> None:
        """
        Handle account synchronization requests.
        
        Expected message format:
        {
            "account_ids": ["account1", "account2", ...] (optional, sync all if not provided),
            "include_children": true/false (default: true)
        }
        """
        try:
            data = json.loads(msg.data.decode())
            account_ids = data.get("account_ids")
            include_children = data.get("include_children", True)
            
            logger.info(f"Account sync requested for {len(account_ids) if account_ids else 'all'} accounts")
            
            # If no specific account IDs provided, this would typically fetch from a master list
            # For now, we'll return an error if no account_ids provided
            if not account_ids:
                await self._send_error_response(msg, "account_ids must be provided for sync")
                return
            
            # Fetch accounts in batches
            accounts = await self.opentext_service.get_accounts_batch(account_ids)
            
            # Include child accounts if requested
            all_accounts = accounts.copy()
            if include_children:
                for account in accounts:
                    if account.child_accounts:
                        child_accounts = await self.opentext_service.get_accounts_batch(
                            account.child_accounts
                        )
                        all_accounts.extend(child_accounts)
            
            # Convert accounts to dict format
            accounts_data = [account.to_dict() for account in all_accounts]
            
            await self._send_response(msg, {
                "accounts": accounts_data,
                "total_count": len(accounts_data),
                "include_children": include_children
            })
            
            logger.info(f"Account sync completed: {len(accounts_data)} accounts")
            
        except json.JSONDecodeError:
            await self._send_error_response(msg, "Invalid JSON in request")
        except Exception as e:
            logger.error(f"Error in account sync: {e}")
            await self._send_error_response(msg, str(e))

    async def _handle_account_get(self, msg: Msg) -> None:
        """
        Handle single account retrieval requests.
        
        Expected message format:
        {
            "account_id": "account123"
        }
        """
        try:
            data = json.loads(msg.data.decode())
            account_id = data.get("account_id")
            
            if not account_id:
                await self._send_error_response(msg, "account_id is required")
                return
            
            logger.info(f"Account get requested for {account_id}")
            
            account = await self.opentext_service.get_account(account_id)
            
            if account:
                await self._send_response(msg, {"account": account.to_dict()})
            else:
                await self._send_error_response(msg, f"Account {account_id} not found")
            
        except json.JSONDecodeError:
            await self._send_error_response(msg, "Invalid JSON in request")
        except Exception as e:
            logger.error(f"Error in account get: {e}")
            await self._send_error_response(msg, str(e))

    # Fax Usage Handlers

    async def _handle_fax_usage_get(self, msg: Msg) -> None:
        """
        Handle fax usage retrieval requests.
        
        Expected message format:
        {
            "account_id": "account123",
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-01-31T23:59:59Z"
        }
        """
        try:
            data = json.loads(msg.data.decode())
            account_id = data.get("account_id")
            start_date_str = data.get("start_date")
            end_date_str = data.get("end_date")
            
            if not all([account_id, start_date_str, end_date_str]):
                await self._send_error_response(
                    msg, "account_id, start_date, and end_date are required"
                )
                return
            
            start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
            end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
            
            logger.info(f"Fax usage get requested for {account_id} ({start_date} to {end_date})")
            
            fax_usage = await self.opentext_service.get_fax_usage(
                account_id, start_date, end_date
            )
            
            if fax_usage:
                await self._send_response(msg, {"fax_usage": fax_usage.to_dict()})
            else:
                await self._send_error_response(
                    msg, f"Fax usage not found for account {account_id}"
                )
            
        except (json.JSONDecodeError, ValueError) as e:
            await self._send_error_response(msg, f"Invalid request format: {e}")
        except Exception as e:
            logger.error(f"Error in fax usage get: {e}")
            await self._send_error_response(msg, str(e))

    async def _handle_fax_usage_sync(self, msg: Msg) -> None:
        """
        Handle fax usage synchronization requests.
        
        Expected message format:
        {
            "account_ids": ["account1", "account2", ...],
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-01-31T23:59:59Z"
        }
        """
        try:
            data = json.loads(msg.data.decode())
            account_ids = data.get("account_ids")
            start_date_str = data.get("start_date")
            end_date_str = data.get("end_date")
            
            if not all([account_ids, start_date_str, end_date_str]):
                await self._send_error_response(
                    msg, "account_ids, start_date, and end_date are required"
                )
                return
            
            start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
            end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
            
            logger.info(f"Fax usage sync requested for {len(account_ids)} accounts")
            
            usage_records = await self.opentext_service.sync_fax_usage(
                account_ids, start_date, end_date
            )
            
            usage_data = [usage.to_dict() for usage in usage_records]
            
            await self._send_response(msg, {
                "fax_usage_records": usage_data,
                "total_count": len(usage_data)
            })
            
            logger.info(f"Fax usage sync completed: {len(usage_data)} records")
            
        except (json.JSONDecodeError, ValueError) as e:
            await self._send_error_response(msg, f"Invalid request format: {e}")
        except Exception as e:
            logger.error(f"Error in fax usage sync: {e}")
            await self._send_error_response(msg, str(e))

    # Number Porting Handlers

    async def _handle_porting_status(self, msg: Msg) -> None:
        """
        Handle porting status requests.
        
        Expected message format:
        {
            "phone_numbers": ["1234567890", "0987654321", ...] (or single "phone_number")
        }
        """
        try:
            data = json.loads(msg.data.decode())
            phone_numbers = data.get("phone_numbers")
            single_number = data.get("phone_number")
            
            if single_number:
                phone_numbers = [single_number]
            
            if not phone_numbers:
                await self._send_error_response(
                    msg, "phone_numbers or phone_number is required"
                )
                return
            
            logger.info(f"Porting status requested for {len(phone_numbers)} numbers")
            
            porting_records = await self.opentext_service.batch_porting_status(phone_numbers)
            
            porting_data = [porting.to_dict() for porting in porting_records]
            
            # For single number requests, return just the record
            if single_number and len(porting_data) == 1:
                await self._send_response(msg, {"porting": porting_data[0]})
            else:
                await self._send_response(msg, {
                    "porting_records": porting_data,
                    "total_count": len(porting_data)
                })
            
        except json.JSONDecodeError:
            await self._send_error_response(msg, "Invalid JSON in request")
        except Exception as e:
            logger.error(f"Error in porting status: {e}")
            await self._send_error_response(msg, str(e))

    async def _handle_porting_update(self, msg: Msg) -> None:
        """
        Handle porting status update requests.
        
        Expected message format:
        {
            "phone_number": "1234567890",
            "status": "completed",
            "notes": "Port completed successfully",
            "completion_date": "2024-01-15T10:00:00Z" (optional)
        }
        """
        try:
            data = json.loads(msg.data.decode())
            phone_number = data.get("phone_number")
            status = data.get("status")
            
            if not all([phone_number, status]):
                await self._send_error_response(
                    msg, "phone_number and status are required"
                )
                return
            
            logger.info(f"Porting update requested for {phone_number}: {status}")
            
            # Get current porting record
            porting = await self.opentext_service.get_porting_status(phone_number)
            if not porting:
                await self._send_error_response(
                    msg, f"Porting record not found for {phone_number}"
                )
                return
            
            # Update the record
            try:
                porting.status = PortingStatus(status)
            except ValueError:
                await self._send_error_response(msg, f"Invalid status: {status}")
                return
            
            if "notes" in data:
                porting.notes = data["notes"]
            
            if "completion_date" in data and data["completion_date"]:
                porting.completion_date = datetime.fromisoformat(
                    data["completion_date"].replace('Z', '+00:00')
                )
            
            # Update via service
            success = await self.opentext_service.update_porting_status(porting)
            
            if success:
                await self._send_response(msg, {"porting": porting.to_dict()})
            else:
                await self._send_error_response(msg, "Failed to update porting status")
            
        except (json.JSONDecodeError, ValueError) as e:
            await self._send_error_response(msg, f"Invalid request format: {e}")
        except Exception as e:
            logger.error(f"Error in porting update: {e}")
            await self._send_error_response(msg, str(e))

    # Usage Aggregation Handler

    async def _handle_usage_aggregate(self, msg: Msg) -> None:
        """
        Handle usage aggregation requests.
        
        Expected message format:
        {
            "account_ids": ["account1", "account2", ...],
            "usage_type": "fax_pages_sent",
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-01-31T23:59:59Z"
        }
        """
        try:
            data = json.loads(msg.data.decode())
            account_ids = data.get("account_ids")
            usage_type_str = data.get("usage_type")
            start_date_str = data.get("start_date")
            end_date_str = data.get("end_date")
            
            if not all([account_ids, usage_type_str, start_date_str, end_date_str]):
                await self._send_error_response(
                    msg, "account_ids, usage_type, start_date, and end_date are required"
                )
                return
            
            try:
                usage_type = UsageDataType(usage_type_str)
            except ValueError:
                await self._send_error_response(msg, f"Invalid usage_type: {usage_type_str}")
                return
            
            start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
            end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
            
            logger.info(
                f"Usage aggregation requested for {len(account_ids)} accounts, "
                f"type: {usage_type_str}"
            )
            
            aggregation = await self.opentext_service.aggregate_usage(
                account_ids, usage_type, start_date, end_date
            )
            
            await self._send_response(msg, {"aggregation": aggregation.to_dict()})
            
            logger.info(
                f"Usage aggregation completed: {aggregation.total_quantity} total quantity"
            )
            
        except (json.JSONDecodeError, ValueError) as e:
            await self._send_error_response(msg, f"Invalid request format: {e}")
        except Exception as e:
            logger.error(f"Error in usage aggregation: {e}")
            await self._send_error_response(msg, str(e))

    # Health Check Handler

    async def _handle_health_check(self, msg: Msg) -> None:
        """Handle health check requests."""
        try:
            logger.info("Health check requested")
            
            # Get health status from OpenText service
            health_status = await self.opentext_service.health_check()
            
            # Add NATS-specific health information
            health_status.update({
                "nats_connected": self.nats.is_connected,
                "active_subscriptions": len(self._subscriptions),
                "service": "centerfuze-opentext-service"
            })
            
            await self._send_response(msg, health_status)
            
        except Exception as e:
            logger.error(f"Error in health check: {e}")
            await self._send_error_response(msg, str(e))

    # Cleanup Methods

    async def close_subscriptions(self) -> None:
        """Close all NATS subscriptions."""
        logger.info("Closing NATS subscriptions")
        
        for subscription in self._subscriptions:
            try:
                await subscription.unsubscribe()
            except Exception as e:
                logger.error(f"Error closing subscription: {e}")
        
        self._subscriptions.clear()
        logger.info("All NATS subscriptions closed")