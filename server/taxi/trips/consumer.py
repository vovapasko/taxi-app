from channels.generic.websocket import AsyncJsonWebsocketConsumer
from trips.serializers import TripSerializer, ReadOnlyTripSerializer
from channels.db import database_sync_to_async
import asyncio

class TaxiConsumer(AsyncJsonWebsocketConsumer):
    # new
    def __init__(self, scope):
        super().__init__(scope)

        # Keep track of the user's trips.
        self.trips = set()

    async def connect(self):
        user = self.scope['user']
        if user.is_anonymous:
            await self.close()
        else:
            await self.accept()

    async def receive_json(self, content, **kwargs):
        message_type = content.get('type')
        if message_type == 'create.trip':
            await self.create_trip(content)
    
    # new
    async def echo_message(self, event):
        await self.send_json(event)


    async def create_trip(self, event):
        trip = await self._create_trip(event.get('data'))
        trip_id = f'{trip.id}'
        trip_data = ReadOnlyTripSerializer(trip).data

        # Add trip to set.
        self.trips.add(trip_id)

        # Add this channel to the new trip's group.
        await self.channel_layer.group_add(
            group=trip_id,
            channel=self.channel_name
        )

        await self.send_json({
            'type': 'create.trip',
            'data': trip_data
        })

     # new
    async def disconnect(self, code):
        # Remove this channel from every trip's group.
        channel_groups = [
            self.channel_layer.group_discard(
                group=trip,
                channel=self.channel_name
            )
            for trip in self.trips
        ]
        asyncio.gather(*channel_groups)

        # Remove all references to trips.
        self.trips.clear()

        await super().disconnect(code)

    @database_sync_to_async
    def _create_trip(self, content):
        serializer = TripSerializer(data=content)
        serializer.is_valid(raise_exception=True)
        trip = serializer.create(serializer.validated_data)
        return trip
