import json
import attr

NUM_PARTITIONS = 10


@attr.s(repr=False)
class MonitoringRecord:
    """
    This class embodies both raw monitoring record constructed from a line of comma-separated values and an enriched
    monitoring record, constructed from the json reply of the htm network.
    """
    stateCode = attr.attr(type=str)
    countryCode = attr.attr(type=str)
    siteNum = attr.attr(type=str)
    parameterCode = attr.attr(type=str)
    poc = attr.attr(type=str)
    latitude = attr.attr(type=str)
    longitude = attr.attr(type=str)
    datum = attr.attr(type=str)
    parameterName = attr.attr(type=str)
    dateLocal = attr.attr(type=str)
    timeLocal = attr.attr(type=str)
    dateGMT = attr.attr(type=str)
    timeGMT = attr.attr(type=str)
    sampleMeasurement = attr.attr(type=str)
    unitsOfMeasure = attr.attr(type=str)
    mdl = attr.attr(type=str)
    uncertainty = attr.attr(type=str)
    qualifier = attr.attr(type=str)
    methodType = attr.attr(type=str)
    methodCode = attr.attr(type=str)
    methodName = attr.attr(type=str)
    stateName = attr.attr(type=str)
    countyName = attr.attr(type=str)
    dateOfLastChange = attr.attr(type=str)
    prediction = attr.attr(type=float, default=0.0, converter=float)
    error = attr.attr(type=float, default=0.0, converter=float)
    anomaly = attr.attr(type=float, default=0.0, converter=float)
    predictionNext = attr.attr(type=float, default=0.0, converter=float)

    @staticmethod
    def from_json(json_record):
        """This method is used to construct a new MonitoringRecord from some json input.

        :param json_record: Input monitoring record in json format.
        :type json_record: json.
        :return MonitoringRecord.
        """
        record_dict = json.loads(json_record)
        return MonitoringRecord(**record_dict)

    def __repr__(self):
        return ','.join(map(str, attr.astuple(self)))

    def get_key(self):
        """This method is used to create a key for the monitoring record, which will define to which of the
        NUM_PARTITIONS partitions this record will go.

        :return: int -- the int number in 0..NUM_PARTITIONS-1
        """
        return '-'.join([self.stateCode, self.countryCode, self.siteNum, self.parameterCode, self.poc])
