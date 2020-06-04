import com.google.common.reflect.Reflection;
import com.google.j2objc.annotations.ReflectionSupport;
import com.google.protobuf.ByteString;
import com.test.example.OOMGrpc;
import com.test.example.Test;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoop;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.NettyNanoTime;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        Server start = NettyServerBuilder
                .forPort(8081)
                .maxInboundMessageSize(1024*1024*100)
                .keepAliveTime(5,TimeUnit.SECONDS)
                .keepAliveTimeout(50,TimeUnit.SECONDS)
                .addService(createService())
                .build().start();

        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(16);
        ManagedChannel localhost = NettyChannelBuilder
                .forAddress("localhost", 8081)
                .usePlaintext()
                .keepAliveWithoutCalls(true)
                .maxInboundMessageSize(1024*1024*100)
                .keepAliveTime(5,TimeUnit.SECONDS)
                .keepAliveTimeout(50,TimeUnit.SECONDS)
                .channelType(NioSocketChannel.class)
                .eventLoopGroup(eventLoopGroup).build();
        // reference to ClientCall
        // will captured by future created here
        // io.grpc.internal.ClientCallImpl.startDeadlineNotifyApplicationTimer
        OOMGrpc.OOMStub oomStub = OOMGrpc.newStub(localhost)
                .withDeadlineAfter(2, TimeUnit.HOURS);

        byte[] body =  new byte[1*1024*1024];
        CountDownLatch l =new CountDownLatch(200);
        for (int i = 0; i < 200; i++) {
            oomStub.submit(Test.Request.newBuilder()
                    .setBody(ByteString.copyFrom(body))
                    .build(), new StreamObserver<Test.Response>() {
                @Override
                // here body(which can be huge) captured by  anonymous class
                public void onNext(Test.Response value) {
                    System.out.println("Response received"+body.length);
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("Error ");
                    t.printStackTrace();

                }

                @Override
                public void onCompleted() {
                    l.countDown();
                    System.out.println("Completed ");
                }
            });
        }
        l.await();
        // 1) after all task submitted ( no network occurred and no RPC issued
        // NioEventGroup will poll  selector  with timeout which will be  calculated
        // as deadline of nearest ScheduledFutureTask() in our case 2 hours
        //  NioEventLoop.nextWakeupNanos  tell us when nex wakeup occurs in case of absence of other task or network events
        // 2) After completion of all request all deadline timers will be cancelled
        // and ShceduledTasks will be LAZY executed by  netty
        // LAZY means when any other schduled task will be scheduled or network event occurs
        Thread.sleep(10000);
        eventLoopGroup.forEach(e->{
            NioEventLoop e1 = (NioEventLoop) e;
            Queue<Runnable>  queue =  getFieldValue(e1,"taskQueue");
            Map<String, Long> collect = queue.stream().collect(Collectors.groupingBy(Object::toString, Collectors.counting()));
            AtomicLong nextWakeup =  getFieldValue(e1,"nextWakeupNanos");
            long deadline =  Duration.ofNanos((nextWakeup.get())).toSeconds();
            long now = Duration.ofNanos(NettyNanoTime.nanoTime()).toSeconds();
            System.out.println("####################################");
            System.out.println(e1+"--- Next wakeup after "+(deadline-now) +" second ");

            System.out.println("Tasks:");
            collect.forEach((k,v)-> System.out.println("\t"+k+"-"+v));
        });
        // as result we see a lot of cancellation tasks in queue of each NioEventLoop
        // which will be removed after 2 hours in case of tasks absence
        start.awaitTermination();

    }

    private static <T> T getFieldValue(NioEventLoop e1, String taskQueue) {
        Class<?> klass = e1.getClass();
        while(klass!=null){
            try {
                Field[] fields = klass.getDeclaredFields();
                for (Field field : fields) {
                    if(field.getName().equals(taskQueue)){
                        field.setAccessible(true);
                        return (T)field.get(e1);
                    }
                }
                klass = klass.getSuperclass();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return null;

    }

    private static BindableService createService() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
        return new OOMGrpc.OOMImplBase() {
            @Override
            public void submit(Test.Request request, StreamObserver<Test.Response> responseObserver) {
                scheduledExecutorService.schedule(()->{
                        responseObserver.onNext(Test.Response.getDefaultInstance());
                    responseObserver.onCompleted();
                },5,TimeUnit.SECONDS);
            }
        };
    }
}
