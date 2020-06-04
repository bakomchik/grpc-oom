package io.grpc.netty.shaded.io.netty.util.concurrent;

public class NettyNanoTime {
    public static long nanoTime(){
        return ScheduledFutureTask.nanoTime();
    }
}
