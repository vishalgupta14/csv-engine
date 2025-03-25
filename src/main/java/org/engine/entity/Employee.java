package org.engine.entity;

public class Employee {
    public int id;
    public String name;
    public int department_id;
    public int salary;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getDepartment_id() {
        return department_id;
    }

    public void setDepartment_id(int department_id) {
        this.department_id = department_id;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "👤 " + name + " | 🆔 " + id + " | 🏢 Dept: " + department_id + " | 💰 Salary: " + salary;
    }
}
