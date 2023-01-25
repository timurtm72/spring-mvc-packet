package ru.maxima.springmvc.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.maxima.springmvc.models.Person;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Component
public class PersonDAO {
    private int PEOPLE_COUNT;
    private JdbcTemplate jdbcTemplate;
//    private final String URL = "jdbc:postgresql://localhost:5432/my_db";
//    private final String USERNAME = "postgres";
//    private final String PASSWORD = "timur_2022";
//    private Connection connection;
//    private List<Person> people;

//    public PersonDAO() {
//        try {
//            Class.forName("org.postgresql.Driver");
//            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
//        } catch (ClassNotFoundException | SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
    @Autowired
    public PersonDAO(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Person> index() {
//        List<Person> people = new ArrayList<>();
//        try {
//            Statement statement = connection.createStatement();
//            String SQL = "select * from person";
//            ResultSet resultSet = statement.executeQuery(SQL);
//            while(resultSet.next()){
//                Person person = new Person();
//                person.setId(resultSet.getInt("id"));
//                person.setName(resultSet.getString("name"));
//                person.setAge(resultSet.getInt("age"));
//                person.setEmail(resultSet.getString("email"));
//                people.add(person);
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        return people;
        return jdbcTemplate.query("select * from person", new BeanPropertyRowMapper<>(Person.class));
    }


    public Person show(int id) {
//        Person person = new Person();
//        try {
//            String SQL = "select * from person where id = ?";
//            //Statement statement = connection.createStatement();
//            PreparedStatement preparedStatement = connection.prepareStatement(SQL);
//            preparedStatement.setInt(1,id);
//            ResultSet resultSet = preparedStatement.executeQuery();
//            while(resultSet.next()){
//                person.setId(resultSet.getInt("id"));
//                person.setName(resultSet.getString("name"));
//                person.setAge(resultSet.getInt("age"));
//                person.setEmail(resultSet.getString("email"));
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        return person;
        return jdbcTemplate.query("select * from person where id= ?", new Object[]{id}, new BeanPropertyRowMapper<>(Person.class)).
                stream().findAny().orElse(null);
    }

    public void save(Person person) {
//        try {
//            Statement statement = connection.createStatement();
//            String SQL = "insert into person values (" + ++PEOPLE_COUNT + ", '" + person.getName() +
//                     "', " + person.getAge() + ", '" + person.getEmail()  + "')";
//            String SQL = "insert into person values (1,?,?,?)";
//            PreparedStatement preparedStatement = connection.prepareStatement(SQL);
//            preparedStatement.setString(1, person.getName());
//            preparedStatement.setInt(2, person.getAge());
//            preparedStatement.setString(3, person.getEmail());
//            preparedStatement.executeUpdate();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
        jdbcTemplate.update("insert into person values(1,?,?,?)",person.getName(),
                person.getAge(),person.getEmail());
    }

    public void update(int id, Person updatedPerson) {
//        Person personToBeUpdated = show(id);
//        try {
//            Statement statement = connection.createStatement();
//            String SQL = "update  person set name = '"  + updatedPerson.getName() +  "' ,age = "
//                    + updatedPerson.getAge() + ", " + "email = '"  + updatedPerson.getEmail() + "'where id = "
//                    + id;
//            String SQL = "update  person set name = ? ,age = ?, email = ? where id = ?";
//            PreparedStatement prepareStatement = connection.prepareStatement(SQL);
//            prepareStatement.setString(1, updatedPerson.getName());
//            prepareStatement.setInt(2, updatedPerson.getAge());
//            prepareStatement.setString(3, updatedPerson.getEmail());
//            prepareStatement.setInt(4, id);
//            prepareStatement.executeUpdate();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
        jdbcTemplate.update("update person set name = ?, age = ?, email = ? where id = ?",updatedPerson.getName(),
                updatedPerson.getAge(),updatedPerson.getEmail(),id);
    }

    public void delete(int id) {
//        try {
//            Statement statement = connection.createStatement();
//            String SQL = "delete from person where id = " + id;
//            String SQL = "delete from person where id = ?";
//            PreparedStatement preparedStatement = connection.prepareStatement(SQL);
//            preparedStatement.setInt(1, id);
//            preparedStatement.executeUpdate();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
        jdbcTemplate.update("delete from person where id = ?", id);
    }

    public void testWithOutBatch() {
        long start = System.currentTimeMillis();
        List<Person> people = create1000person();
        for (Person person : people) {
            jdbcTemplate.update("insert into person values(?,?,?,?)",person.getId(),
                    person.getName(),person.getAge(),person.getEmail());
        }
        long end = System.currentTimeMillis();
        System.out.println("Without batch update - " + (end - start) + " msec");
    }

    public void testWithBatch() {
        long start = System.currentTimeMillis();
        List<Person> people = create1000person();
        jdbcTemplate.batchUpdate("insert into person values(?,?,?,?)", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                preparedStatement.setInt(1,people.get(i).getId());
                preparedStatement.setString(2,people.get(i).getName());
                preparedStatement.setInt(3,people.get(i).getAge());
                preparedStatement.setString(4,people.get(i).getEmail());
            }

            @Override
            public int getBatchSize() {
                return people.size();
            }
        });
        long end = System.currentTimeMillis();
        System.out.println("With batch update - " + (end - start) + " msec");
    }

    public List<Person> create1000person(){
        List<Person> people = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            people.add(new Person(i, "name" + i, i, "test" + i + "@mail.ru"));
        }
        return people;
    }
}