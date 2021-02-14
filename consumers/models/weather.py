"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("start weather process_message processing")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        if message.topic=="weather-status":
            content=json.load(message.value)
            temperature=content.get('temperature')
            status=content.get('status')
            print(f"weather value processed, temprature value {temperature}, status is {status}")

            
            
        
