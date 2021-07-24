package com.ddwei.folder.CreateFolder;

import java.io.*;

/**
 * CreateFolderAndFileTest
 *
 * @author κ����
 * @date 2021/6/25
 */
public class CreateFolderAndFileTest {
    //�ֱ����ַ���Ŀ¼�ļ�·����Դ�ļ�ǰ׺·���������ļ�ǰ׺·��,��Ŀ¼�ļ����롣ʹ��ʱ�ǵó�ʼ��
    private final String contentPath,sourcePre,targetPre,charset;

    public CreateFolderAndFileTest(){
        contentPath = "C:/Users/weijingli/Desktop/aaa/aaa.txt";
        sourcePre = "F:/kewai";
        targetPre = "C:/Users/weijingli/Desktop";
        charset = "GBK";
    }

    /**
     * ��ȡ�ļ�����
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        CreateFolderAndFileTest test = new CreateFolderAndFileTest();
        File contentFile = new File(test.contentPath);
        if(!contentFile.getParentFile().exists()){
        	System.out.println("Ŀ¼�ļ������ڣ�");
            return;
        }else{
            FileInputStream contentStream = new FileInputStream(contentFile);
            InputStreamReader isr = new InputStreamReader(contentStream, test.charset);
            BufferedReader br = new BufferedReader(isr);
            String lineText;
            while((lineText=br.readLine())!=null){
                test.lineHandle(lineText);
            }
        }


    }

    /**
     * ÿ�д������ַ���������ļ�����
     * @param commonPath
     */
    public void lineHandle(String commonPath){

        File sourceFile,targetFile;
        sourceFile = new File(sourcePre+commonPath);
        targetFile = new File(targetPre+commonPath);
        if(!targetFile.getParentFile().exists()){
            targetFile.getParentFile().mkdirs();
        }
        copyFile(sourceFile,targetFile);
    }

    /**
     * copy�ļ�����
     */
    public void copyFile(File sourceFile,File targetFile){

        try {
            FileInputStream in = new FileInputStream(sourceFile);
            FileOutputStream out = new FileOutputStream(targetFile);

            byte[] bs = new byte[1024];
            int count = 0;

            while(-1!=(count=in.read(bs,0,bs.length))){
                out.write(bs,0,count);
            }
            in.close();
            out.flush();
            out.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}