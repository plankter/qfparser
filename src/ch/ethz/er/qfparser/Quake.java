package ch.ethz.er.qfparser;

/**
 * Created by anton on 21.01.16.
 */
public class Quake {
    int timeSecs;
    double depth;
    double magnitude;
    Coordinate coordinate;

    public Quake(int tim, Coordinate loc, double dep, double mag) {
        coordinate = loc;
        depth = dep;
        magnitude = mag;
        timeSecs = tim;
    }
}
