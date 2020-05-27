package lab10;


import java.sql.Date;

public class Main {

	public static void main(String[] args) {
		AddressDAO addressDAO = new AddressDAO();
		ClientDAO clientDAO = new ClientDAO();

//		Address newAddress = Address.of();
//		newAddress.setId(5L);
//		newAddress.setCountry("Ukraine");
//		newAddress.setCity("Lviv");
//		newAddress.setStreet("Drahomanova");
//		newAddress.setHouse("51");
//
//		addressDAO.save(newAddress);
//
//		Client newClient = Client.of();
//		newClient.setId(555L);
//		newClient.setFirstName("Hideo");
//		newClient.setSecondName("Kojima");
//		newClient.setGender(Gender.MALE);
//		newClient.setStatus(Status.INDIVIDUAL);
//		newClient.setAddress(newAddress);
//
//		clientDAO.save(newClient);

//		addressDAO.findById(5L).ifPresent(System.out::println);
//		addressDAO.findFirstByFieldValue("city", "Lviv").ifPresent(System.out::println);
//
//		clientDAO.findById(555L).ifPresent(System.out::println);
//		clientDAO.findFirstByFieldValue("firstname", "Hideo").ifPresent(System.out::println);
//
//		clientDAO.findById(555L).ifPresent(client -> {
//			client.setBirthDate(new Date(1963, 8, 24));
//			client.setPhoneNumber("(532) 532-01-12");
//			clientDAO.update(client);
//		});
//
//		addressDAO.findById(5L).ifPresent(address -> {
//			address.setIndex(79004);
//			address.setState("Lvivska oblast");
//			addressDAO.update(address);
//		});
//
		addressDAO.remove(5L);
		clientDAO.remove(555L);
	}
}
