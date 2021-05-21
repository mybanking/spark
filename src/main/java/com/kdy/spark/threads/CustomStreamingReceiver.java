package com.kdy.spark.threads;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION: 自定义接收streaming类
 * @USER: 14020
 * @DATE: 2021/4/8 1:31
 *
 */
@Slf4j
public class CustomStreamingReceiver extends Receiver<String> {

    private static final long serialVersionUID = 5817531198342629801L;

    public CustomStreamingReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
        new Thread(()->{
            doStart();
        }).start();
        log.info("开始启动Receiver...");
    }

    public void doStart() {
        while(!isStopped()) {
            //随机产生0-100 数值
            int value = RandomUtils.nextInt(100);
            if(value <20) {
                try {
                    Thread.sleep(1000);
                }catch (Exception e) {
                    log.error("sleep exception",e);
                    restart("sleep exception", e);
                }
            }
            store(String.valueOf(value));
        }
    }


    @Override
    public void onStop() {
        log.info("即将停止Receiver...");
    }
}

