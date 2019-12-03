package com.aaa.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/12/2
 */
public class test {
    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(1,2,3,4,5);
        System.out.println("----------Arrays.asList----------");
        for (Integer i : list) {
            System.out.println(i);
        }
        list.set(0, 10);
        System.out.println("----------set修改----------");
        for (Integer i : list) {
            System.out.println(i);
        }


        list.add(0, 0);
        System.out.println("----------add添加----------");
        for (Integer i : list) {
            System.out.println(i);
        }
        list.remove(0);
        System.out.println("----------remove删除----------");
        for (Integer i : list) {
            System.out.println(i);
        }

    }
}
