from py4j.java_gateway import JavaGateway, java_import
from json.decoder import JSONDecoder


class ResultRecord:
    """
    This class embodies an enriched monitoring record, constructed from the json reply of the htm network.
    """
    def __init__(self, json_record):
        decoder = JSONDecoder()
        values = decoder.decode(json_record)
        self.state_code = values['stateCode']
        self.country_code = values['countryCode']
        self.site_num = values['siteNum']
        self.parameter_code = values['parameterCode']
        self.poc = values['poc']
        self.lat = values['latitude']
        self.lon = values['longitude']
        self.datum = values['datum']
        self.parameter_name = values['parameterName']
        self.date_local = values['dateLocal']
        self.time_local = values['timeLocal']
        self.date_gmt = values['dateGMT']
        self.time_gmt = values['timeGMT']
        self.sample_measurement = values['sampleMeasurement']
        self.units_of_measure = values['unitsOfMeasure']
        self.mdl = values['mdl']
        self.uncertainty = values['uncertainty']
        self.qualifier = values['qualifier']
        self.method_type = values['methodType']
        self.method_code = values['methodCode']
        self.method_name = values['methodName']
        self.state_name = values['stateName']
        self.county_name = values['countyName']
        self.date_of_last_change = values['dateOfLastChange']
        self.prediction = float(values['prediction'])
        self.error = float(values['error'])
        self.anomaly = float(values['anomaly'])
        self.prediction_next = float(values['predictionNext'])

    def __repr__(self):
        return ','.join([self.state_code, self.country_code, self.site_num, self.parameter_code, self.poc, self.lat,
                         self.lon, self.datum, self.parameter_name, self.date_local, self.time_local, self.date_gmt,
                         self.time_gmt, self.sample_measurement, self.units_of_measure, self.mdl, self.uncertainty,
                         self.qualifier, self.method_type, self.method_code, self.method_name, self.state_name,
                         self.date_of_last_change, self.prediction, self.error, self.anomaly, self.prediction_next])


def send_to_network(gateway, line):
    """This function sends a monitoring record to a Java Gateway and receives the reply as a json.

    :param gateway: JavaGateway to use.
    :type gateway: JavaGateway.
    :param line: Input records as a line of comma-separated values.
    :type line: str.
    :return: json.
    """
    values = line.split(',') + [0.0, 0.0, 0.0, 0.0]
    raw_record = gateway.jvm.MonitoringRecord(*values)
    record = gateway.entry_point.mappingFunc('1', raw_record).toJson()
    return record


if __name__ == '__main__':
    gateway = JavaGateway()
    while True:
        line = input()
        result = send_to_network(gateway, line)
        print(ResultRecord(result))
