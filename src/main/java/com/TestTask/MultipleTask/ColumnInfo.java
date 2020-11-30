package com.TestTask.MultipleTask;

/**данный класс содержит необходимую структуру для описания столбцов таблицы
  */
public class ColumnInfo
{
    private String Name;
    private String Tipe;

    String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    String getTipe() {
        return Tipe;
    }

    public void setTipe(String tipe) {
        Tipe = tipe;
    }

    ColumnInfo(String name, String tipe) {
        Name = name;
        Tipe = tipe;
    }
}