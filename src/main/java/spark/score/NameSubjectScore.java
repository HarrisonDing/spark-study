package spark.score;

import java.io.Serializable;

public class NameSubjectScore implements Serializable {
	private static final long	serialVersionUID	= -6097505670372662849L;
	private String				name;
	private String				subject;
	private Float				score;

	public NameSubjectScore(String name, String subject, Float score) {
		super();
		this.name = name;
		this.subject = subject;
		this.score = score;
	}

	public NameSubjectScore() {
		super();
	}

	public NameSubjectScore parseLine(String line) {
		String[] lineArray = line.split("\t");
		if (lineArray.length < 1)
			return null;

		return new NameSubjectScore(lineArray[0], lineArray[1], Float.valueOf(lineArray[2]));
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public Float getScore() {
		return score;
	}

	public void setScore(Float score) {
		this.score = score;
	}
}
