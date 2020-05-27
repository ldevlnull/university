package lab10;

import lombok.Data;

@Data(staticConstructor = "of")
public class Address implements Entity {

	private Long id;
	private String country;
	private String state;
	private String city;
	private Integer index;
	private String street;
	private String house;
	private String phoneNumber;
}
