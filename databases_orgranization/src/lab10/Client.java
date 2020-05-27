package lab10;

import lombok.Data;

import java.sql.Date;

@Data(staticConstructor = "of")
public class Client implements Entity {

	private Long id;
	private String firstName;
	private String secondName;
	private Gender gender;
	private Date birthDate;
	private String phoneNumber;
	private Status status;

	private Address address;
}
