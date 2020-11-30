package com.TestTask.MultipleTask;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Logs
{
    static void WriteLog(String log)
    {
        try
        {
            BufferedWriter bfw = new BufferedWriter(new FileWriter("log.txt",true));
            bfw.write(log);
            bfw.newLine();
            bfw.close();
        }catch (IOException e){System.out.println("Не удалось записать log. Строка:"+log);}
    }
    static void LogInit()
    {
        if (Files.exists(Paths.get("log.txt")))
        {
            try {
                Files.delete(Paths.get("log.txt"));
            }catch (IOException e)
            {
                System.out.println("Не удалось очистить логи");
            }
        }
        try {
            Files.createFile(Paths.get("log.txt"));
        }catch (IOException e){System.out.println("Не удалось создать файл log.txt");}
    }

}
