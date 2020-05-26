package lab8;

import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.System.out;

public class Main {

	private enum SQLFunc {
		AVG("avg"), MAX("max"), MIN("min");

		private final String val;

		SQLFunc(String val) {
			this.val = val;
		}
	}

	private enum SQLFilter {
		EQUAL("="), IN("IN"), GREATER(">"), LESS("<");

		private final String val;

		SQLFilter(String val) {
			this.val = val;
		}
	}

	private static final class SQLPredicate<T> {
		private final String column;
		private final SQLFilter filter;
		private final T value;

		private enum Concat {
			AND("and"), OR("or");

			private final String val;

			Concat(String val) {
				this.val = val;
			}
		}

		private final Map<SQLPredicate<?>, Concat> nextPredicates = new LinkedHashMap<>();

		public SQLPredicate(String column, SQLFilter filter, T value) {
			this.column = column;
			this.filter = filter;
			this.value = value;
		}

		private SQLPredicate<T> and(SQLPredicate<T> predicate) {
			nextPredicates.put(predicate, Concat.AND);
			return this;
		}

		private SQLPredicate<T> or(SQLPredicate<T> predicate) {
			nextPredicates.put(predicate, Concat.OR);
			return this;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();

			builder.append(column).append(" ").append(filter.val).append(" ").append(resolveValue(this));

			nextPredicates.forEach((p, concat) -> builder.append(" ").append(concat).append(" ")
					.append(p.column).append(" ").append(p.filter.val).append(" ").append(resolveValue(p)));

			return " WHERE " + builder.toString();
		}

		private static String resolveValue(SQLPredicate<?> predicate) {
			if (predicate.value instanceof String)
				return String.format("'%s'", predicate.value);
			else if (predicate.value instanceof Collection)
				return ((Collection<?>) predicate.value)
						.stream()
						.map(o -> resolveValue(new SQLPredicate<>(null, null, o)))
						.collect(Collectors.toSet())
						.toString().replaceAll("\\[", "(").replaceAll("]", ")");
			else
				return predicate.value.toString();
		}
	}

	private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/";
	private static final String DATABASE_SCHEME = "lawyersdb";
	private static final String DATABASE_USERNAME = System.getenv("DB_USERNAME");
	private static final String DATABASE_PASSWORD = System.getenv("DB_PASSWORD");

	private static final Scanner sc = new Scanner(System.in);

	private static boolean showSQL = true;

	private static Connection connection;

	public static void main(String[] args) throws SQLException {
		registerDriver();
		connect();

		while (true) {
			showMenu();
			out.println("Select...");
			int userInput = sc.nextInt();
			switch (userInput) {
				case 0:
					showSQL = !showSQL;
					out.println("Show SQL turned " + (showSQL ? "on" : "off"));
					break;
				case 1:
					getTableNames().forEach(out::println);
					break;
				case 2:
					out.println("Input table name...");
					String tableName = sc.next();
					getTableStructure(tableName).entrySet().forEach(out::println);
					break;
				case 3:
					out.println("Input table name...");
					tableName = sc.next();
					out.println("Input column name...");
					String columnName = sc.next();
					out.println("Choose math function...\n" + Arrays.toString(SQLFunc.values()));
					out.println("1 - first, 2 - second...");
					int mathFuncIndex = sc.nextInt() - 1;
					out.println("Result: " + getCalculatedColumnValue(SQLFunc.values()[mathFuncIndex], tableName, columnName));
					break;
				case 4:
					out.println("Input table name...");
					tableName = sc.next();
					out.println("Input column name which to order by...");
					columnName = sc.next();
					out.println("Descending? (y/n)");
					boolean isDescending = confirm();
					getOrderedResult(tableName, columnName, isDescending).forEach(out::println);
					break;
				case 5:
					out.println("Input table name...");
					tableName = sc.next();
					SQLPredicate<Object> rootFilter = null;
					boolean appendWithAND = false;
					while (true) {
						out.println("Enter filtering column...");
						columnName = sc.next();
						out.println("Enter filtering action...");
						out.println(Arrays.toString(SQLFilter.values()));
						out.println("1 - first, 2 - second...");
						int filterFuncIndex = sc.nextInt() - 1;
						out.println("Is your value a list? (y/n)");
						boolean isValueList = confirm();
						out.println("Is your value(s) string(s)? (y/n)");
						boolean isValueString = confirm();
						out.println("Enter filtering value");
						final Object wrappedValue;
						if (!isValueList) {
							String filterValue = sc.next();
							wrappedValue = (isValueString) ? filterValue : Double.parseDouble(filterValue);
						} else {
							if (isValueString) {
								Set<String> values = new HashSet<>();
								do {
									out.println("input 1 value");
									values.add(sc.next());
									out.println("stop? (y/n)");
								} while (!sc.next().equalsIgnoreCase("y"));
								wrappedValue = values;
							} else {
								Set<Double> values = new HashSet<>();
								do {
									out.println("input 1 value");
									values.add(sc.nextDouble());
									out.println("stop? (y/n)");
								} while (!sc.next().equalsIgnoreCase("y"));
								wrappedValue = values;
							}
						}
						if (rootFilter == null)
							rootFilter = new SQLPredicate<>(columnName, SQLFilter.values()[filterFuncIndex], wrappedValue);
						else {
							if (appendWithAND)
								rootFilter.and(new SQLPredicate<>(columnName, SQLFilter.values()[filterFuncIndex], wrappedValue));
							else
								rootFilter.or(new SQLPredicate<>(columnName, SQLFilter.values()[filterFuncIndex], wrappedValue));
						}

						out.println("Append one more action with AND/OR? (y/n)");
						if (confirm()) {
							while (true) {
								out.println("Append with AND or OR?");
								String concatAction = sc.next();
								if (concatAction.equalsIgnoreCase("AND")) {
									appendWithAND = true;
									break;
								} else if (concatAction.equalsIgnoreCase("OR")) {
									appendWithAND = false;
									break;
								} else
									out.println("Bad input!");
							}
						} else {
							break;
						}
					}
					getFilteredResult(tableName, rootFilter).forEach(out::println);
					break;
				case 6:
					out.println("Input table name...");
					tableName = sc.next();
					Set<String> columnNames = getTableStructure(tableName).keySet();
					Map<String, Serializable> valuesToInsert = new HashMap<>();
					columnNames.forEach(column -> {
						out.println("Input for " + column);
						String val = sc.next();
						if (val.equals("null")) {
							valuesToInsert.put(column, val);
						} else {
							out.println("Is your inputted value a string? (y/n)");
							if (confirm()) {
								valuesToInsert.put(column, val);
							} else {
								valuesToInsert.put(column, Double.parseDouble(val));
							}
						}
					});
					insert(tableName, valuesToInsert);
					break;
				case 7:
					out.println("Input table name...");
					tableName = sc.next();
					out.println("Enter column you want to update...");
					columnName = sc.next();
					out.println("Enter id of entity you want to update...");
					int entityId = sc.nextInt();
					out.println("Is your new value string? (y/n)");
					boolean isString = confirm();
					out.println("Enter new value...");
					Serializable newValue = (isString) ? sc.next() : sc.nextDouble();
					updateById(tableName, columnName, newValue, entityId);
					break;
				default:
					disconnect();
					System.exit(1);
			}
		}
	}

	private static boolean confirm() {
		String confirm = sc.next();
		while (true) {
			if (confirm.equalsIgnoreCase("y"))
				return true;
			else if (confirm.equalsIgnoreCase("n"))
				return false;
			else
				confirm = sc.next();
		}
	}

	private static void showMenu() {
		out.println(
				"\n=================================\n" +
						"0. Toggle Show SQL Queries\n" +
						"1. Show All Tables\n" +
						"2. Show Structure of table\n" +
						"3. Retrieve calculated result from table\n" +
						"4. Retrieve ordered result from table\n" +
						"5. Retrieve result with filter\n" +
						"6. Insert row\n" +
						"7. Update row\n" +
						"Other. To quit\n" +
						"====================================\n"
		);
	}

	private static void updateById(String tableName, String columnName, Serializable newValue, int entryId) throws SQLException {
		checkConnection();
		if (newValue instanceof String)
			newValue = "'" + newValue + "'";
		final String query = String.format("UPDATE %s SET %s = %s WHERE ID = %d", tableName, columnName, newValue, entryId);
		try (Statement statement = connection.createStatement()) {
			log(query);
			statement.execute(query);
		}
	}

	private static void insert(String tableName, Map<String, ? extends Serializable> data) throws SQLException {
		checkConnection();
		final StringBuilder columns = new StringBuilder("(");
		final StringBuilder values = new StringBuilder("(");
		data.forEach((col, val) -> {
			columns.append(col).append(", ");
			if (val instanceof String && !((String) val).equalsIgnoreCase("null"))
				values.append("'").append(val).append("', ");
			else
				values.append(val).append(", ");
		});
		columns.replace(columns.length() - 2, columns.length(), ")");
		values.replace(values.length() - 2, values.length(), ")");
		final StringBuilder query = new StringBuilder("INSERT INTO ").append(tableName)
				.append(columns).append(" VALUES ").append(values);
		try (Statement statement = connection.createStatement()) {
			log(query);
			statement.execute(query.toString());
		}
	}

	private static Set<String> getOrderedResult(String tableName, String orderByColumn, boolean desc) throws SQLException {
		checkConnection();
		final String query = String.format("SELECT * FROM %s ORDER BY %s %s", tableName, orderByColumn, desc ? "ASC" : "DESC");
		try (Statement statement = connection.createStatement()) {
			log(query);
			return queryStringToString(tableName, statement.executeQuery(query));
		}
	}

	private static <T> Set<String> getFilteredResult(String tableName, SQLPredicate<T> pred) throws SQLException {
		checkConnection();
		final String query = String.format("SELECT * FROM %s %s", tableName, pred);
		try (Statement statement = connection.createStatement()) {
			log(query);
			return queryStringToString(tableName, statement.executeQuery(query));
		}
	}

	private static Set<String> queryStringToString(String tableName, ResultSet resultSet) throws SQLException {
		final int columnCount = getTableStructure(tableName).size();
		return new LinkedHashSet<>() {{
			while (resultSet.next()) {
				StringBuilder builder = new StringBuilder();
				for (int i = 1; i <= columnCount; i++) {
					builder.append(String.format("%1$20s", resultSet.getString(i)));
				}
				add(builder.toString());
			}
		}};
	}

	private static double getCalculatedColumnValue(SQLFunc mathFunc, String tableName, String columnName) throws SQLException {
		checkConnection();
		final String query = String.format("SELECT %s(%s) FROM %s", mathFunc.val, columnName, tableName);
		try (Statement statement = connection.createStatement()) {
			log(query);
			ResultSet resultSet = statement.executeQuery(query);
			double avg = 0;
			while (resultSet.next())
				avg += resultSet.getDouble(1);
			return avg;
		}
	}

	private static Set<String> getTableNames() throws SQLException {
		checkConnection();
		try (Statement statement = connection.createStatement()) {
			final String query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
			log(query);
			ResultSet result = statement.executeQuery(query);
			return new HashSet<>() {{
				while (result.next())
					add(result.getString("tablename"));
			}};
		}
	}

	private static Map<String, Map<String, String>> getTableStructure(String tableName) {
		checkConnection();
		final String SQL_TABLE_STRUCTURE = "SELECT\n" +
				"a.attname AS Field,\n" +
				"t.typname || '(' || a.atttypmod || ')' AS Type,\n" +
				"CASE WHEN a.attnotnull = 't' THEN 'YES' ELSE 'NO' END AS Null,\n" +
				"CASE WHEN r.contype = 'p' THEN 'PRI' ELSE '' END AS Key,\n" +
				"(SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '\"(.*)\"')\n" +
				"FROM\n" +
				"pg_catalog.pg_attrdef d\n" +
				"WHERE\n" +
				"d.adrelid = a.attrelid\n" +
				"AND d.adnum = a.attnum\n" +
				"AND a.atthasdef) AS Default,\n" +
				"'' as Extras\n" +
				"FROM\n" +
				"pg_class c \n" +
				"JOIN pg_attribute a ON a.attrelid = c.oid\n" +
				"JOIN pg_type t ON a.atttypid = t.oid\n" +
				"LEFT JOIN pg_catalog.pg_constraint r ON c.oid = r.conrelid \n" +
				"AND r.conname = a.attname\n" +
				"WHERE\n" +
				"c.relname = '%s'\n" +
				"AND a.attnum > 0\n" +
				"\n" +
				"ORDER BY a.attnum;";

		try (final Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery(String.format(SQL_TABLE_STRUCTURE, tableName));
			return new HashMap<>() {{
				while (resultSet.next()) {
					Map<String, String> props = new HashMap<>() {{
						put("type", resultSet.getString("type"));
						put("nullable", resultSet.getString("null"));
						put("default", resultSet.getString("default"));
						put("key", resultSet.getString("key"));
						put("extras", resultSet.getString("extras"));
					}};
					props.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());
					put(resultSet.getString("field"), props);
				}
			}};
		} catch (SQLException e) {
			e.printStackTrace();
			return Collections.emptyMap();
		}
	}

	private static void checkConnection() {
		if (connection == null)
			throw new IllegalStateException("Not connected to database. You need to connect first!");
	}

	private static void disconnect() throws SQLException {
		connection.close();
	}

	private static void connect() throws SQLException {
		connection = DriverManager.getConnection(DATABASE_URL + DATABASE_SCHEME, DATABASE_USERNAME, DATABASE_PASSWORD);
		out.println("Successfully connected to " + DATABASE_URL + DATABASE_SCHEME);
	}

	private static void registerDriver() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void log(Object obj) {
		if (showSQL)
			out.println("DEBUG: " + obj);
	}
}
