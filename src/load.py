from src.logs import Logs
class Load:
    def __init__(self, data_frame):
        self.dataFrame = data_frame
        self.logger = Logs.get_logger("load")
    def save_as_csv(self):
        self.logger.info("saving in csv file in progress...")
        self.dataFrame.write.csv("./data/cleaned/uber-cleaned.csv")
        self.logger.info("data frame uber was saving")