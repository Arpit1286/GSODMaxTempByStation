package org.arpit.mapreduce;

import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Arpit on 2/26/2015.
 */
public class GSODStationParser {

    private Map<String, String> stationIdToName = new HashMap<String, String>();

    public void initialize(File file) throws IOException {
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            GSODStationMetaDataParser parser = new GSODStationMetaDataParser();
            String line;
            while ((line = in.readLine()) != null) {
                if (parser.parse(line)) {
                    stationIdToName.put(parser.getStationID(), parser.getStationName());
                }
            }
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public String getStationName(String stationId) {
        String stationName = stationIdToName.get(stationId);
        if ( stationName == null || stationName.trim().length() == 0) {
            return stationId;  // return in case of no match
        }
        return stationName;
    }

    public Map<String, String> getStationIdToNameMap() {
        return Collections.unmodifiableMap(stationIdToName);
    }
}
