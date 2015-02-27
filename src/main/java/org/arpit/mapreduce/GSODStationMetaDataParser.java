package org.arpit.mapreduce;

import org.apache.hadoop.io.Text;

/**
 * Created by Arpit on 2/26/2015.
 */
public class GSODStationMetaDataParser {
     private String stationID;
    private String stationName;
    private String Latitude;
    private String Longitude;
    private String CountryCode;

    public boolean parse(String record) {

        if (record.length() < 69) {
            return false;
        }
        String usaf = record.substring(0, 7);
        String wban = record.substring(7, 12);
        stationID = usaf + "-" + wban;
        stationName = record.substring(13, 42);
        Latitude = record.substring(51,59);
        Longitude = record.substring(60,69);
        CountryCode = record.substring(44,46);
        try {
            Integer.parseInt(usaf);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean parse(Text record) {
        return parse(record.toString());
    }

    public String getStationID() {
        return stationID;
    }

    public String getStationName() {
        return stationName;
    }

    public String getLatitude() {
        return Latitude;
    }

    public String getLongitude() {
        return Longitude;
    }

    public String getCountryCode(){
        return CountryCode;
    }
}

