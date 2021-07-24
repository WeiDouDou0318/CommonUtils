package com.ddwei.sql.sqlrelase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 功能：
 * 
 * 1	根据select,delete,update 等生成DML
 * 1.1	不满足sql语法会退出
 * 1.2	select 查询结果为空会退出
 * 1.3	有clob,blob类型字段会退出
 * 1.4	自动替换&为'||'&'||'
 * 1.5	sql报错会退出
 * 
 * 2	根据DML生成bat并且自动执行，有报错停止并退出，同时指出相关报错地方
 * 
 * 记得在init方法初始化参数，才能开始测试
 * 
 * add by ddwei 
 * 20210722
 * 
 * 一个重要的sql语句，虽然没有用到，但是感觉非常棒，可以做另一条思路。就记下了
 * select '('
||''''||serialno||''''||','
||''''||customerid||''''||','
||''''||customername||''''||','
||''''||inputuserid||''''||','
||''''||businesssum||''''||','
||''''||inputdate||''''||','
||''''||updatedate||''''||');'
from business_apply;

 * @author xh_dengjl
 *
 */
public class MakeSqlFileTest {
	
	//分别定义字符串目录文件路径，源文件前缀路径，生成文件前缀路径,和目录文件编码。使用时记得初始化
	private static String userId,passWord,url,contentPath,sourcePre,executeDataConfig,executeSql,sourcePreTemp;

	private static final String CHARSET = "GBK";
	private static final String DRIVER_NAME = "oracle.jdbc.OracleDriver"; 
	private static final String EXECUTE_ROLLBACK = "whenever sqlerror exit rollback;"; 
	private static String tableName;
	static PreparedStatement ps = null;
	static PreparedStatement ps1 = null;
	static ResultSet rs = null;
	static ResultSet rs1 = null;

	
	/**
	 * 需要执行sql时打开executeSql即可，注掉makeSql即可
	 * @param args
	 * @throws Exception 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		init();
		
		//生成sql文件
		//makeSql();
		
		//执行sql
		executeSql();
		
	}
	
	/**
	 * 执行sql语句
	 * @throws IOException
	 */
	public static void executeSql() throws IOException{
		
		
		//1		CopyDML 到 DMLTemp
		File contentFile = new File(sourcePre);

		System.out.println("bbb");
		FileInputStream contentStream = new FileInputStream(contentFile);
		
		InputStreamReader isr = new InputStreamReader(contentStream, CHARSET);
		BufferedReader br = new BufferedReader(isr);
		String sqlLine;

		BufferedWriter targetbw = new BufferedWriter(new FileWriter(sourcePreTemp));
		targetbw.write(EXECUTE_ROLLBACK+"\r\n");

		while((sqlLine=br.readLine()) != null){
			targetbw.write(sqlLine+"\r\n");
		}
		targetbw.close();
		System.out.println("bbb");
		
		//2		生成bat
		String cmd = "@echo off\r\n"+executeDataConfig+executeSql+"\r\n"
		+"pause";
		String url = "C:/Users/John/Desktop/aaa/查看ip.bat";
		FileWriter fw = null;
		
		try {
			fw = new FileWriter(url);
			fw.write(cmd);
			fw.close();
			
			//3		运行bat文件
			Process process = Runtime.getRuntime().exec(url);
			InputStream in = process.getInputStream();
			String line;
			BufferedReader brNew = new BufferedReader(new InputStreamReader(in));
			while((line = brNew.readLine()) != null){
				System.out.println(line);
			}
			in.close();
			process.waitFor();
			System.out.println("执行成功！");
		} catch (InterruptedException e) {
			// TODO 自动生成 catch 块
			e.printStackTrace();
		}
		
		
	}
	
	/**
	 * 生成DML
	 * @throws Exception
	 */
	public static void makeSql() throws Exception{
		try {
			File contentFile = new File(contentPath);
			if(!contentFile.getParentFile().exists()){
				System.out.println("目录文件不存在！");
				return;
			}else{
				FileInputStream contentStream = new FileInputStream(contentFile);
				
				InputStreamReader isr = new InputStreamReader(contentStream, CHARSET);
				BufferedReader br = new BufferedReader(isr);
				String sqlLine;
	
				BufferedWriter targetbw = new BufferedWriter(new FileWriter(sourcePre));
				
				//打开连接：
				Class.forName(DRIVER_NAME);//加载驱动
				Connection conn = DriverManager.getConnection(url,userId,passWord); //获得连接
				
				while((sqlLine=br.readLine()) != null){
					
					//1		select 语句
					if(sqlLine.toUpperCase().startsWith("SELECT")){
						//1.1		添加注释 并 打印出select 
						targetbw.write("--"+sqlLine+"\r\n");
						
						//1.2		添加delete语句
						targetbw.write(sqlLine.replace("select *", "delete")+"\r\n");
						
						//1.3		添加insert语句
						tableName = getTableName(sqlLine);
						ps = conn.prepareStatement(sqlLine.replaceAll(";", ""));
						rs = ps.executeQuery();
						if(rs.isBeforeFirst()==rs.isAfterLast()){
							System.out.println("警告：未查询到语句\r\n"+sqlLine);
							return;
						}
						
						//1.3.1	查到该条数据
						while(rs.next()){
							String sqlInsertPre = "";//insert前半段语句
							sqlInsertPre = "insert into "+tableName+" (";//insert前半段语句
							String sqlInsertSuffix = "values (";
							
							//1.3.2	查询表结构，根据表机构循环进行拼接insert语句
							String sqlDescribe = "select * from USER_TAB_COLUMNS A WHERE A.TABLE_NAME = '"+tableName+"'";
							ps1 = conn.prepareStatement(sqlDescribe);
							rs1 = ps1.executeQuery();
							while(rs1.next()){
								String column = rs1.getString(2);//column
								String dataType = rs1.getString(3);//datatype
								String dataScale = NulltoString(rs1.getString("DATA_SCALE"));
								sqlInsertPre += (column + ", ");
								if(dataType.contains("CHAR")){//字符串类型 char varchar
									if(rs.getString(column)==null){
										sqlInsertSuffix += ("'',");
									}else{
										sqlInsertSuffix += NulltoString("'" + rs.getString(column) + "',");
									}
								}else if("NUMBER".equals(dataType)){//int类型Or Double类型
									if("".equals(dataScale)){
										sqlInsertSuffix += (rs.getInt(column) + ",");
									}else{
										sqlInsertSuffix += (rs.getDouble(column) + ",");
									}
								}else if(dataType.contains("CLOB")||dataType.contains("BLOB")){
									System.out.println("错误：clob类型或者blob类型数据不能通过insert方式进行抽取DML！\r\n该sql语句为"+sqlLine);
									return;
								}else{
									sqlInsertSuffix += ("'" + rs.getString(column) + "',");
								}
							}
							sqlInsertPre = (sqlInsertPre.substring(0, sqlInsertPre.length()-2)+")\r\n");	//去掉最后一个逗号
							sqlInsertSuffix = (sqlInsertSuffix.substring(0, sqlInsertSuffix.length()-1)+");\r\n").replaceAll("&", "'||'&'||'");	//去掉最后一个逗号
							
							//1.4	添加insert语句前半部分
							targetbw.write(sqlInsertPre);
							
							//1.5	添加insert语句后半部分
							targetbw.write(sqlInsertSuffix);
							
							//1.6	添加一处空行进行分隔
							targetbw.write("\r\n");
						}
						
					//2		update语句或注释处理
					}else if(sqlLine.toUpperCase().startsWith("UPDATE") ||sqlLine.toUpperCase().startsWith("DELETE") || sqlLine.startsWith("--")||"".equals(sqlLine)){
						targetbw.write(sqlLine+"\r\n");
					
					//3		其他情况
					}else{
						throw new Exception("sql异常，异常的sql语句："+sqlLine);
					}
					
					System.out.println("以下sql语句已执行成功"+sqlLine);
				}
				System.out.println("Conguatulations!您已运行完毕");
				targetbw.close();
			}
		} catch (FileNotFoundException e) {
			System.out.println("文件未找到");
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			System.out.println("编码格式不对");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("请核实文件是否存在");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("请核查Oracle驱动相关");
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("请核查数据库用户名，密码，ip等是否录入正确 或者看成功语句排查出问题的语句");
			e.printStackTrace();
		} 
	}
	
	public static void init(){
		userId = "l7wd0708";								//Oracle用户名
		passWord = "l7wd0708";								//Oracle密码
		url = "jdbc:oracle:thin:@193.158.170.21:1521:orcl";	//Oracle url
		contentPath = "C:/Users/John/Desktop/aaa/aaa.txt";	//生成DML:原文件路径
		sourcePre = "C:/Users/John/Desktop/aaa/DML.sql";	//生成DML:生成文件路径
		
		executeDataConfig = "sqlplus l7wd0708/l7wd0708@193.158.170.21:1521/orcl";	//执行DML 配置相关
		sourcePreTemp = "C:/Users/John/Desktop/aaa/DMLTemp.sql";					//执行DML 临时sql,用于报错停止
		executeSql = " @"+sourcePreTemp;												//执行DML 执行sql语句
		
	}

	/**
	 * 获取表名
	 * @param sqlLine
	 * @return
	 */
	public static String getTableName(String sqlLine){
		String sqlSuffix = sqlLine.substring(sqlLine.substring(0,sqlLine.indexOf("from ")).length()+5);
		return sqlSuffix.substring(0,sqlSuffix.indexOf(" ")).toUpperCase();
	}
	
	/**
	 * 空值处理
	 * @param sStr
	 * @return
	 */
	private static String NulltoString(String sStr){
		if(sStr==null)	return "";
		else return sStr;
	}
	
}
