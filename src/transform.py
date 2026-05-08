# print(df.groupby('Booking ID').count().filter("COUNT > 1").show(150000))
# df = df.dropDuplicates(['Booking ID'])
# print(f'total of rows after remove duplicates : {df.count()}') # 150000

from src.logs import Logs

class Transform:
    def __init__(self, data_frame):
        self.dataFrame = data_frame
        self.logger = Logs.get_logger("transform")

    def info(self):
        count = self.dataFrame.count()
        types = self.dataFrame.dtypes

        print(count)
        print(types)

        self.logger.info(f"count of rows before cleaning and duplicating : {count}")
        self.logger.info(f"type of columns {types}")

    def remove_duplicates(self):
        count_duplicated = self.dataFrame.groupBy('Booking ID').count().filter("COUNT > 1").count()
        self.dataFrame = self.dataFrame.dropDuplicates(['Booking ID'])
        self.logger.info(f"{count_duplicated} row was removed because is duplicated")
        self.logger.info(f"{self.dataFrame.count()} unique rows")
