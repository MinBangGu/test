package com.bridgetct.core;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class test {

		public static void main(String[] args) {
			Date date = new Date();
//			  Calendar cal = Calendar.getInstance();
//			  cal.setTime(date);
//			  cal.add(Calendar.DATE, -25);
			  System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
//			  String ddd ="zskjdlakjsd sadsd";
//			  System.out.println(ddd.endsWith("a"));
//			System.out.println("시작");
//			new B().two();
		}
}

class A{
	@Deprecated
	static void one(){
		System.out.println("클래스 A의 one 메소드");
	}
	void two(){
		System.out.println("클래스 A의 two 메소드");
	}
}

class B extends A{
	
	@Override
	void two(){
		System.out.println("클래스 B의 two 메소드");
	}
}