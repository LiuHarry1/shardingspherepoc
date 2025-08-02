package org.example;

public class VolatileVisibilityTest {

//    private static volatile boolean initFlag =false;
    private static boolean initFlag =false;
//    private  static User user = new User();


    public static void main(String[] args) throws InterruptedException {

        new Thread(() ->{
            System.out.println("waiting data .. ");
            while(!initFlag){

            }
            System.out.print("==========success");

        }).start();

        Thread.sleep(2000);

        new Thread(() ->prepareData()).start();

        Thread.sleep(2000);
        System.out.println(initFlag);
    }

    public static void prepareData(){
        System.out.println("Starting to init");
        initFlag = true;
        System.out.println("Finished to init");
    }



}
