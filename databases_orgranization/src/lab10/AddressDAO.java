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

public class AddressDAO extends DAO<Address, Long> {

	@Override
	public Address save(Address address) {
		final String sql = "INSERT INTO address(id, country, state, city, index, street, house, phone_number) " +
				"VALUES (%d,'%s','%s','%s', %d,'%s','%s','%s')";
		PreparedStatement ps = getPrepareStatement(String.format(sql, address.getId(), address.getCountry(),
				address.getState(), address.getCity(), address.getIndex(), address.getStreet(), address.getHouse(), address.getPhoneNumber()));
		try {
			ps.execute();
			return ps.getUpdateCount() > 0 ? address : null;
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return null;
	}

	@Override
	public Optional<Address> findById(Long id) {
		Address address = null;
		PreparedStatement ps = getPrepareStatement("SELECT * FROM address WHERE ID = " + id);
		try {
			ResultSet res = ps.executeQuery();
			while (res.next()) {
				address = Address.of();
				extractAddressFromResultSet(address, res);
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return Optional.ofNullable(address);
	}


	@Override
	public List<Address> findAll() {
		List<Address> addresses = new LinkedList<>();
		PreparedStatement ps = getPrepareStatement("SELECT * FROM address");
		try {
			ResultSet res = ps.executeQuery();
			while (res.next()) {
				Address address = Address.of();
				extractAddressFromResultSet(address, res);
				addresses.add(address);
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return addresses;
	}

	private void extractAddressFromResultSet(Address address, ResultSet res) throws SQLException {
		address.setId(res.getLong("id"));
		address.setCountry(res.getString("country"));
		address.setState(res.getString("state"));
		address.setCity(res.getString("city"));
		address.setStreet(res.getString("street"));
		address.setHouse(res.getString("house"));
		address.setPhoneNumber(res.getString("phone_number"));
	}

	@Override
	public Address update(Address address) {
		String sql = "UPDATE address SET " +
				"country = '%s'," +
				"state = '%s'," +
				"city = '%s'," +
				"index = %d," +
				"street = '%s'," +
				"house = '%s'," +
				"phone_number = '%s' " +
				"WHERE ID = %d";

		sql = String.format(sql, address.getCity(), address.getState(), address.getCity(), address.getIndex(),
				address.getStreet(), address.getHouse(), address.getPhoneNumber(), address.getId());
		PreparedStatement ps = getPrepareStatement(sql);
		try {
			ps.execute();
			return address;
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean remove(Long id) {
		PreparedStatement ps = getPrepareStatement("DELETE FROM address WHERE ID = " + id);
		try {
			ps.execute();
			return ps.getUpdateCount() > 0;
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean remove(Address address) {
		return this.remove(address.getId());
	}

	@Override
	public Optional<Address> findFirstByFieldValue(String fieldName, String fieldValue) {
		Set<String> fields = Stream.of("country", "state", "city", "index", "street", "house", "phone_number").collect(Collectors.toSet());
		if (!fields.contains(fieldName))
			throw new IllegalArgumentException("No field " + fieldName + " in class Address found!");

		Address address = null;
		PreparedStatement ps = getPrepareStatement(String.format("SELECT * FROM address WHERE %s = '%s'", fieldName, fieldValue));
		try {
			ResultSet res = ps.executeQuery();
			while (res.next()) {
				address = Address.of();
				extractAddressFromResultSet(address, res);
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
		return Optional.ofNullable(address);
	}
}
