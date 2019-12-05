package kafka.kafka_starter;

public class Student {

	private String name;

	private int rollno;

	private String clazz;

	public Student(String name, int rollno, String clazz) {
		this.name = name;
		this.rollno = rollno;
		this.clazz = clazz;
	}

	public Student() {

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getRollno() {
		return rollno;
	}

	public void setRollno(int rollno) {
		this.rollno = rollno;
	}

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}
}
