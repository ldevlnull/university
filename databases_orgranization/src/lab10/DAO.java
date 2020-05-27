package lab10;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 *
 * @param <E> - type of Entity
 * @param <I> - type of Entity`s id
 */
public abstract class DAO<E extends Entity, I> {

	public abstract E save(E e);
	public abstract Optional<E> findById(I id);
	public abstract List<E> findAll();
	public abstract E update(E e);
	public abstract boolean remove(I id);
	abstract boolean remove(E e);
	public abstract Optional<E> findFirstByFieldValue(String fieldName, String fieldValue);

	private final String DATABASE_URL = "jdbc:postgresql://localhost:5432/";
	private final String DATABASE_SCHEME = "lawyersdb";
	private final String DATABASE_USERNAME = System.getenv("DB_USERNAME");
	private final String DATABASE_PASSWORD = System.getenv("DB_PASSWORD");

	private Connection connection;

	public DAO() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private Connection getConnection() throws SQLException {
		if (connection == null)
			connection = DriverManager.getConnection(DATABASE_URL + DATABASE_SCHEME, DATABASE_USERNAME, DATABASE_PASSWORD);
		return connection;
	}

	public PreparedStatement getPrepareStatement(String sql) {
		PreparedStatement ps = null;
		try {
			ps = getConnection().prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ps;
	}

	public void closePrepareStatement(PreparedStatement ps) {
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
