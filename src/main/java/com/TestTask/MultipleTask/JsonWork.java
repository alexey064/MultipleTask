package com.TestTask.MultipleTask;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class JsonWork
{
    private String FileName;
    private LocalDateTime LowestDate;
    private LocalDateTime HighestDate;
    JsonWork(String FileName)
    {
        this.FileName=FileName;
    }

    private String[] getColumn(int ColumnCount) {
        String[] output = new String[ColumnCount];
        try
        {
            FileReader flr = new FileReader(FileName);
            BufferedReader bfr= new BufferedReader(flr);
            bfr.readLine();
            bfr.readLine();
            String stroka, temp="";
            boolean isWrite=false;
            for (int i=0; i<ColumnCount;i++)
            {
                temp="";
                isWrite=false;
                stroka=bfr.readLine();
                stroka=stroka.split(":")[0];
                for (int j=0; j<stroka.length(); j++)
                {
                    if (stroka.charAt(j)=='\"')
                    {
                        isWrite=!isWrite;
                        continue;
                    }
                    if (isWrite) {temp+=stroka.charAt(j);}
                }
                output[i]=temp;
            }
        }
        catch (IOException e){
            Logs.WriteLog("не удалось прочитать файл:"+FileName);
            return null;
        }
        return output;
    }

        boolean WriteToFile(String DestFile) {
        String[] Row = new String[getParamCount()];
        try {
            if (Files.exists(Paths.get(DestFile))) {
                Files.delete(Paths.get(DestFile));
            }
        } catch (IOException e)
        {
            Logs.WriteLog("Не удалось удалить файл "+DestFile);
            return false;
        }
        int RowCount=0;
        String[] col = getColumn(Row.length);
        if (col==null){return false;}
        if (!AppendRow(DestFile, col))
            {return false;}
        try(BufferedReader bfr = new BufferedReader(new FileReader(FileName)))
        {
            String stroka;
            while ((stroka=bfr.readLine())!=null)
            {
                if (stroka.contains("{"))
                {
                    for (int i=0; i<Row.length; i++)
                    {
                        stroka=bfr.readLine();
                        Row[i]= getvalue(stroka);
                    }

                    if(!AppendRow(DestFile, Row)){return false;}
                    RowCount++;
                    if(!ParseDate(Row[2])){return false;}
                }
            }
        }catch (IOException e)
        {
            Logs.WriteLog("Не удалось прочитать файл "+FileName);
            return false;
        }
            return WriteFooter(DestFile, RowCount);
        }
    private boolean WriteFooter(String DestFile, int RowCount) {
        String stroka;
        stroka = "$" + RowCount + "|";
        stroka += LowestDate.getDayOfMonth() + "." + LowestDate.getMonthValue() + "." + LowestDate.getYear() + " " + LowestDate.getHour() + ":" + LowestDate.getMinute() + ":" + LowestDate.getSecond();
        stroka += "@"+HighestDate.getDayOfMonth() + "." + HighestDate.getMonthValue() + "." + HighestDate.getYear() + " " + HighestDate.getHour() + ":" + HighestDate.getMinute() + ":" + HighestDate.getSecond();
        try(FileWriter wr = new FileWriter(DestFile, true)) {
            wr.append(stroka);
        } catch (IOException e)
        {
            Logs.WriteLog("Не удалось записать в файл "+DestFile);
            return false;
        }
        return true;
    }
    private boolean ParseDate(String StringDate)
    {
        SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS");
        StringDate=StringDate.replace("T","-");
        StringDate=StringDate.replace("Z","");
        LocalDateTime CurrentDate;
        try {
            CurrentDate = sdf.parse(StringDate).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        }catch (Exception ex)
        {
            System.out.println("не удалось преобразовать формат даты");
            return false;
        }
        if (LowestDate==null)
        {
            HighestDate=CurrentDate;
            LowestDate=CurrentDate;
        }
        else
            {
                if (CurrentDate.isBefore(LowestDate)) LowestDate=CurrentDate;
                else if(CurrentDate.isAfter(HighestDate)) HighestDate=CurrentDate;
            }
        return true;
    }

    private boolean AppendRow(String DestFile, String[] values) {
        String stroka="";
        for (int i=0; i<values.length; i++)
        {
            stroka+=values[i];
            if (i<values.length-1) {stroka+=";";}
        }
        try(BufferedWriter bfr = new BufferedWriter(new FileWriter(DestFile, true)))
        {
            bfr.write(stroka);
            bfr.newLine();
        } catch (IOException e)
        {
            Logs.WriteLog("Не удалось записать в файл "+DestFile);
            return false;
        }
        return true;
    }
    private String getvalue(String stroka)
    {
        boolean IsWrite=false;
        String output ="";
        String temp = stroka.split(":")[0];
        stroka=stroka.replace(temp+":","");
        if (stroka.contains("\""))
        {
            for (int i=0; i<stroka.length(); i++)
            {
                if (stroka.charAt(i)=='\"')
                {
                    i++;
                    while (stroka.charAt(i) != 34) {
                        if (stroka.charAt(i) != 32) //32=пробел
                        {
                            output += stroka.charAt(i);
                        }
                        i++;
                    }
                    return output;
                }
            }
        }
        else for (int i=0; i<stroka.length(); i++)
        {
            if (stroka.charAt(i)==' ' || stroka.charAt(i)==',')
            {
                IsWrite=!IsWrite;
                continue;
            }
            if (IsWrite)
            {
                output+=stroka.charAt(i);
            }
        }
        return output;
    }

    private int getParamCount() {
        String stroka;
        int output=0;
        try (BufferedReader bfr = new BufferedReader(new FileReader(FileName)))
        {
            while ((stroka=bfr.readLine())!=null)
            {
                if (stroka.contains("{"))
                {
                    stroka=bfr.readLine();
                    while (!stroka.contains("},"))
                    {
                        output++;
                        stroka=bfr.readLine();
                    }
                    return output;
                }
            }
        } catch (IOException e)
        {
            Logs.WriteLog("Не удалось прочитать файл "+FileName);
            return 0;
        }
        return 0;
    }
}