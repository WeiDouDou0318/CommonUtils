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
 * ���ܣ�
 * 
 * 1	����select,delete,update ������DML
 * 1.1	������sql�﷨���˳�
 * 1.2	select ��ѯ���Ϊ�ջ��˳�
 * 1.3	��clob,blob�����ֶλ��˳�
 * 1.4	�Զ��滻&Ϊ'||'&'||'
 * 1.5	sql������˳�
 * 
 * 2	����DML����bat�����Զ�ִ�У��б���ֹͣ���˳���ͬʱָ����ر���ط�
 * 
 * �ǵ���init������ʼ�����������ܿ�ʼ����
 * 
 * add by ddwei 
 * 20210722
 * 
 * һ����Ҫ��sql��䣬��Ȼû���õ������Ǹо��ǳ�������������һ��˼·���ͼ�����
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
	
	//�ֱ����ַ���Ŀ¼�ļ�·����Դ�ļ�ǰ׺·���������ļ�ǰ׺·��,��Ŀ¼�ļ����롣ʹ��ʱ�ǵó�ʼ��
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
	 * ��Ҫִ��sqlʱ��executeSql���ɣ�ע��makeSql����
	 * @param args
	 * @throws Exception 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		init();
		
		//����sql�ļ�
		//makeSql();
		
		//ִ��sql
		executeSql();
		
	}
	
	/**
	 * ִ��sql���
	 * @throws IOException
	 */
	public static void executeSql() throws IOException{
		
		
		//1		CopyDML �� DMLTemp
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
		
		//2		����bat
		String cmd = "@echo off\r\n"+executeDataConfig+executeSql+"\r\n"
		+"pause";
		String url = "C:/Users/John/Desktop/aaa/�鿴ip.bat";
		FileWriter fw = null;
		
		try {
			fw = new FileWriter(url);
			fw.write(cmd);
			fw.close();
			
			//3		����bat�ļ�
			Process process = Runtime.getRuntime().exec(url);
			InputStream in = process.getInputStream();
			String line;
			BufferedReader brNew = new BufferedReader(new InputStreamReader(in));
			while((line = brNew.readLine()) != null){
				System.out.println(line);
			}
			in.close();
			process.waitFor();
			System.out.println("ִ�гɹ���");
		} catch (InterruptedException e) {
			// TODO �Զ����� catch ��
			e.printStackTrace();
		}
		
		
	}
	
	/**
	 * ����DML
	 * @throws Exception
	 */
	public static void makeSql() throws Exception{
		try {
			File contentFile = new File(contentPath);
			if(!contentFile.getParentFile().exists()){
				System.out.println("Ŀ¼�ļ������ڣ�");
				return;
			}else{
				FileInputStream contentStream = new FileInputStream(contentFile);
				
				InputStreamReader isr = new InputStreamReader(contentStream, CHARSET);
				BufferedReader br = new BufferedReader(isr);
				String sqlLine;
	
				BufferedWriter targetbw = new BufferedWriter(new FileWriter(sourcePre));
				
				//�����ӣ�
				Class.forName(DRIVER_NAME);//��������
				Connection conn = DriverManager.getConnection(url,userId,passWord); //�������
				
				while((sqlLine=br.readLine()) != null){
					
					//1		select ���
					if(sqlLine.toUpperCase().startsWith("SELECT")){
						//1.1		���ע�� �� ��ӡ��select 
						targetbw.write("--"+sqlLine+"\r\n");
						
						//1.2		���delete���
						targetbw.write(sqlLine.replace("select *", "delete")+"\r\n");
						
						//1.3		���insert���
						tableName = getTableName(sqlLine);
						ps = conn.prepareStatement(sqlLine.replaceAll(";", ""));
						rs = ps.executeQuery();
						if(rs.isBeforeFirst()==rs.isAfterLast()){
							System.out.println("���棺δ��ѯ�����\r\n"+sqlLine);
							return;
						}
						
						//1.3.1	�鵽��������
						while(rs.next()){
							String sqlInsertPre = "";//insertǰ������
							sqlInsertPre = "insert into "+tableName+" (";//insertǰ������
							String sqlInsertSuffix = "values (";
							
							//1.3.2	��ѯ��ṹ�����ݱ����ѭ������ƴ��insert���
							String sqlDescribe = "select * from USER_TAB_COLUMNS A WHERE A.TABLE_NAME = '"+tableName+"'";
							ps1 = conn.prepareStatement(sqlDescribe);
							rs1 = ps1.executeQuery();
							while(rs1.next()){
								String column = rs1.getString(2);//column
								String dataType = rs1.getString(3);//datatype
								String dataScale = NulltoString(rs1.getString("DATA_SCALE"));
								sqlInsertPre += (column + ", ");
								if(dataType.contains("CHAR")){//�ַ������� char varchar
									if(rs.getString(column)==null){
										sqlInsertSuffix += ("'',");
									}else{
										sqlInsertSuffix += NulltoString("'" + rs.getString(column) + "',");
									}
								}else if("NUMBER".equals(dataType)){//int����Or Double����
									if("".equals(dataScale)){
										sqlInsertSuffix += (rs.getInt(column) + ",");
									}else{
										sqlInsertSuffix += (rs.getDouble(column) + ",");
									}
								}else if(dataType.contains("CLOB")||dataType.contains("BLOB")){
									System.out.println("����clob���ͻ���blob�������ݲ���ͨ��insert��ʽ���г�ȡDML��\r\n��sql���Ϊ"+sqlLine);
									return;
								}else{
									sqlInsertSuffix += ("'" + rs.getString(column) + "',");
								}
							}
							sqlInsertPre = (sqlInsertPre.substring(0, sqlInsertPre.length()-2)+")\r\n");	//ȥ�����һ������
							sqlInsertSuffix = (sqlInsertSuffix.substring(0, sqlInsertSuffix.length()-1)+");\r\n").replaceAll("&", "'||'&'||'");	//ȥ�����һ������
							
							//1.4	���insert���ǰ�벿��
							targetbw.write(sqlInsertPre);
							
							//1.5	���insert����벿��
							targetbw.write(sqlInsertSuffix);
							
							//1.6	���һ�����н��зָ�
							targetbw.write("\r\n");
						}
						
					//2		update����ע�ʹ���
					}else if(sqlLine.toUpperCase().startsWith("UPDATE") ||sqlLine.toUpperCase().startsWith("DELETE") || sqlLine.startsWith("--")||"".equals(sqlLine)){
						targetbw.write(sqlLine+"\r\n");
					
					//3		�������
					}else{
						throw new Exception("sql�쳣���쳣��sql��䣺"+sqlLine);
					}
					
					System.out.println("����sql�����ִ�гɹ�"+sqlLine);
				}
				System.out.println("Conguatulations!�����������");
				targetbw.close();
			}
		} catch (FileNotFoundException e) {
			System.out.println("�ļ�δ�ҵ�");
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			System.out.println("�����ʽ����");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("���ʵ�ļ��Ƿ����");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("��˲�Oracle�������");
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("��˲����ݿ��û��������룬ip���Ƿ�¼����ȷ ���߿��ɹ�����Ų����������");
			e.printStackTrace();
		} 
	}
	
	public static void init(){
		userId = "l7wd0708";								//Oracle�û���
		passWord = "l7wd0708";								//Oracle����
		url = "jdbc:oracle:thin:@193.158.170.21:1521:orcl";	//Oracle url
		contentPath = "C:/Users/John/Desktop/aaa/aaa.txt";	//����DML:ԭ�ļ�·��
		sourcePre = "C:/Users/John/Desktop/aaa/DML.sql";	//����DML:�����ļ�·��
		
		executeDataConfig = "sqlplus l7wd0708/l7wd0708@193.158.170.21:1521/orcl";	//ִ��DML �������
		sourcePreTemp = "C:/Users/John/Desktop/aaa/DMLTemp.sql";					//ִ��DML ��ʱsql,���ڱ���ֹͣ
		executeSql = " @"+sourcePreTemp;												//ִ��DML ִ��sql���
		
	}

	/**
	 * ��ȡ����
	 * @param sqlLine
	 * @return
	 */
	public static String getTableName(String sqlLine){
		String sqlSuffix = sqlLine.substring(sqlLine.substring(0,sqlLine.indexOf("from ")).length()+5);
		return sqlSuffix.substring(0,sqlSuffix.indexOf(" ")).toUpperCase();
	}
	
	/**
	 * ��ֵ����
	 * @param sStr
	 * @return
	 */
	private static String NulltoString(String sStr){
		if(sStr==null)	return "";
		else return sStr;
	}
	
}
