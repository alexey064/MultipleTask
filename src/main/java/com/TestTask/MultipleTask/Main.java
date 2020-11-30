package com.TestTask.MultipleTask;


import java.nio.file.Files;
import java.nio.file.Paths;

public class Main
{

    public static void main(String[] args) throws Exception {
        Logs.LogInit();
        if (!SQLProcedure.IsDBCreated())
            {
                System.out.println("База данных не найдена. Создаем базу данных");
                Logs.WriteLog("База данных не найдена. Создаем базу данных");
                if (!SQLProcedure.SQLDataBaseCreate())
                {
                    throw new Exception("Не удалось создать базу данных");
                }
            }
            Thread th1 = new Thread(Main::FirstTask);
            Thread th2 = new Thread(Main::SecondTask);
            Thread th3 = new Thread(Main::ThirdTask);
            Logs.WriteLog("запускаем task1");
            th1.start();
            Logs.WriteLog("запускаем task2");
            th2.start();
            Logs.WriteLog("запускаем task3");
            th3.start();

            th1.join();
            th2.join();
            th3.join();
            System.out.println("Работа выполнена");
        }
        private static void FirstTask() {
            System.out.println("Первая задача запущена");
            Logs.WriteLog("task1: задача запущена");
            JsonWork json = new JsonWork("Users.json");
            Logs.WriteLog("task1: переменная json создана");
            if(!json.WriteToFile("UserConverted.txt"))
            {
                System.out.println("не удалось выполнить задачу 1. Чтобы узнать подробности посмотрите файл log.txt");
            }
            else System.out.println("Первая задача выполнена");
        }
        private static void SecondTask()  {
            System.out.println("вторая задача запущена");
            Logs.WriteLog("вторая задача запущена");
            String file1 = "file1.xml";
            if (!Files.exists(Paths.get("file1.xml")))
            {
                System.out.println("Не найден файл "+file1);
                Logs.WriteLog("Task2: Не найден файл "+file1);
                System.out.println("не удалось выполнить задачу 2. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            XmlFileReader xml1=null;
            Logs.WriteLog("task2: Создаем объект xml1");
            xml1 = new XmlFileReader(file1,"task2");
            if (!xml1.XmlInit())
            {
                System.out.println("Не удалось прочитать файл " + file1 + " или файл не соответствует заданной структуре");
                Logs.WriteLog("Не удалось прочитать файл " + file1 + " или файл не соответствует заданной структуре");
                System.out.println("не удалось выполнить задачу 2. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            Logs.WriteLog("task2: создаем sql код таблицы"+ xml1.TableName+" в файл "+ xml1.FilePath + "TableCreate.sql");
            Logs.WriteLog("task2: запускаем метод WriteTableHeaderProcedure");
            if(!SQLProcedure.WriteTableHeaderProcedure(xml1))
            {
                Logs.WriteLog("task2: не удалось выполнить метод WriteTableHeaderProcedure");
                System.out.println("не удалось выполнить метод WriteTableHeaderProcedure xml1");
                System.out.println("не удалось выполнить задачу 2. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            Logs.WriteLog("task2: Создаем таблицу "+xml1.TableName);
            if (!SQLProcedure.CreateTable(xml1.FilePath))
            {
                Logs.WriteLog("task2: не удалось выполнить метод CreateTable");
                System.out.println("не удалось выполнить метод CreateTable xml1");
                System.out.println("не удалось выполнить задачу 2. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            if(!xml1.ReadFile())
            {
                Logs.WriteLog("task2: не удалось выполнить метод ReadFile класса XmlFileReader");
                System.out.println("не удалось выполнить метод ReadFile класса XmlFileReader xml1");
                System.out.println("не удалось выполнить задачу 2. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            if(!SQLProcedure.WriteToDB(xml1.TableName, xml1.FilePath))
            {
                Logs.WriteLog("task2: не удалось выполнить метод WriteToDB");
                System.out.println("не удалось выполнить метод WriteToDB xml1");
                System.out.println("не удалось выполнить задачу 2. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            if(!SQLProcedure.AccomplishProcedure(xml1.TableName))
            {
                Logs.WriteLog("task2: не удалось выполнить метод AccomplishProcedure");
                System.out.println("не удалось выполнить метод AccomplishProcedure xml1");
                System.out.println("не удалось выполнить задачу 2. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            Logs.WriteLog("вторая задача выполнена");
            System.out.println("вторая задача выполнена");
        }

        public static void ThirdTask()  {
            System.out.println("третья задача запущена");
            Logs.WriteLog("task3: задача запущена");
            String file2 = "file2.xml";
            XmlFileReader xml2= new XmlFileReader(file2,"task3");
            if(!xml2.XmlInit())
            {
                System.out.println("task3: не удалось выполнить метод Xml2.XmlInit");
                Logs.WriteLog("task3: Не удалось прочитать файл " + file2 + " или файл не соответствует заданной структуре");
                System.out.println("не удалось выполнить задачу 3. Чтобы узнать подробности посмотрите файл log.txt");
            }
            if(!SQLProcedure.WriteTableHeaderProcedure(xml2))
            {
                Logs.WriteLog("Task3: не удалось выполнить метод WriteTableHeaderProcedure");
                System.out.println("не удалось выполнить метод WriteTableHeaderProcedure");
                System.out.println("не удалось выполнить задачу 3. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            if(!SQLProcedure.CreateTable(xml2.FilePath))
            {
                Logs.WriteLog("task3: Не удалось выполнить метод CreateTable xml2");
                System.out.println("Не удалось выполнить метод CreateTable xml2");
                System.out.println("не удалось выполнить задачу 3. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            if(!xml2.ReadFile())
            {
                Logs.WriteLog("task3: Не удалось выполнить метод xml2.ReadFile");
                System.out.println("Не удалось выполнить метод xml2.ReadFile");
                System.out.println("не удалось выполнить задачу 3. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            if(!SQLProcedure.WriteToDB(xml2.TableName, xml2.FilePath))
            {
                Logs.WriteLog("task3: Не удалось выполнить метод WriteToDB xml2");
                System.out.println("Не удалось выполнить WriteToDB xml2");
                System.out.println("не удалось выполнить задачу 3. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }

            if(!SQLProcedure.AccomplishProcedure(xml2.TableName))
            {
                Logs.WriteLog("task3: Не удалось выполнить метод AccomplishProcedure xml2");
                System.out.println("Не удалось выполнить метод AccomplishProcedure xml2");
                System.out.println("не удалось выполнить задачу 3. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            if (!SQLProcedure.Output(xml2))
            {
                Logs.WriteLog("task3: Не удалось выполнить метод Output xml2");
                System.out.println("Не удалось выполнить метод Output xml2");
                System.out.println("не удалось выполнить задачу 3. Чтобы узнать подробности посмотрите файл log.txt");
                return;
            }
            Logs.WriteLog("task3: задача завершена");
            System.out.println("третья задача завершена");
        }
}