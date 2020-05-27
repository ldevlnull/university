package lab10;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClientDAO extends DAO<Client, Long> {

	private final AddressDAO addressDAO = new AddressDAO();

	@Override
	public Client save(Client client) {
		final String sql = "INSERT INTO clients(id, firstname, secondname, gender, birthdate, phone_number, status, address_id) " +
				"VALUES (%d,'%s', '%s', %s, %s, %s, %s, %s)";
		PreparedStatement ps = getPrepareStatement(String.format(sql, client.getId(), client.getFirstName(),
				client.getSecondName(),
				(client.getGender() == null) ? "NULL" : "'" + client.getGender().name().toLowerCase() + "'",
				(client.getBirthDate() == null) ? "NULL" : "'" + client.getBirthDate() + "'",
				client.getPhoneNumber(),
				(client.getStatus() == null) ? "NULL" : "'" + client.getStatus().name().toLowerCase() + "'",
				(client.getAddress() == null) ? "NULL" : client.getAddress().getId()));
		try {
			ps.execute();
			return ps.getUpdateCount() > 0 ? client : null;
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return null;
	}

	@Override
	public Optional<Client> findById(Long id) {
		Client client = null;
		PreparedStatement ps = getPrepareStatement("SELECT * FROM clients WHERE ID = " + id);
		try {
			ResultSet res = ps.executeQuery();
			while (res.next()) {
				client = Client.of();
				extractClientFromResultSet(client, res);
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return Optional.ofNullable(client);
	}

	@Override
	public List<Client> findAll() {
		List<Client> clients = new LinkedList<>();
		PreparedStatement ps = getPrepareStatement("SELECT * FROM clients");
		try {
			ResultSet res = ps.executeQuery();
			while (res.next()) {
				Client client = Client.of();
				extractClientFromResultSet(client, res);
				clients.add(client);
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return clients;
	}

	private void extractClientFromResultSet(Client client, ResultSet res) throws SQLException {
		client.setId(res.getLong("id"));
		client.setFirstName(res.getString("firstname"));
		client.setSecondName(res.getString("secondname"));
		client.setBirthDate(res.getDate("birthdate"));
		if (res.getString("gender") != null)
			client.setGender(Gender.valueOf(res.getString("gender").toUpperCase()));
		if (res.getString("status") != null)
			client.setStatus(Status.valueOf(res.getString("status").toUpperCase().replaceAll(" ", "_")));
		client.setPhoneNumber(res.getString("phone_number"));
		client.setAddress(addressDAO.findById(res.getLong("address_id")).orElse(null));
	}

	@Override
	public Client update(Client client) {
		String sql = "UPDATE clients SET " +
				"firstname = '%s'," +
				"secondname = '%s'," +
				"gender = %s," +
				"birthdate = %s," +
				"phone_number = '%s'," +
				"status = %s," +
				"address_id = %d " +
				"WHERE ID = %d";
		PreparedStatement ps = getPrepareStatement(String.format(sql,
				client.getFirstName(),
				client.getSecondName(),
				(client.getGender() == null) ? "NULL" : "'" + client.getGender().name().toLowerCase() + "'",
				(client.getBirthDate() == null) ? "NULL" : "'" + client.getBirthDate() + "'",
				client.getPhoneNumber(),
				(client.getStatus() == null) ? "NULL" : "'" + client.getStatus().name().toLowerCase() + "'",
				(client.getAddress() == null) ? "NULL" : client.getAddress().getId(),
				client.getId()));
		try {
			ps.execute();
			return client;
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean remove(Long id) {
		PreparedStatement ps = getPrepareStatement("DELETE FROM clients WHERE ID = " + id);
		try {
			ps.execute();
			return ps.getUpdateCount() > 0;
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return false;
	}

	@Override
	boolean remove(Client client) {
		return remove(client.getId());
	}

	@Override
	public Optional<Client> findFirstByFieldValue(String fieldName, String fieldValue) {
		Set<String> fields = Stream.of("firstname", "secondname", "gender", "birthdate", "phone_number", "status", "address_id").collect(Collectors.toSet());
		if (!fields.contains(fieldName))
			throw new IllegalArgumentException("No field " + fieldName + " in class Client found!");

		Client client = null;
		PreparedStatement ps = getPrepareStatement(String.format("SELECT * FROM clients WHERE %s = '%s'", fieldName, fieldValue));
		try {
			ResultSet res = ps.executeQuery();
			while (res.next()) {
				client = Client.of();
				extractClientFromResultSet(client, res);
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return Optional.ofNullable(client);
	}
}
