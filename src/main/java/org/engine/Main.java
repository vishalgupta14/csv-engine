package org.engine;

import org.engine.entity.CsvSource;
import org.engine.entity.Employee;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {

        CsvSource source = CsvSource.fromFile("/home/vishalgupta/test/csv-engine/src/main/resources/employees.csv");

        List<Employee> employees = source.stream().mapTo(Employee.class);
        employees.forEach(System.out::println);

    }

}
