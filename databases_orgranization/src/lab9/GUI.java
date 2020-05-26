package lab9;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.text.AttributeSet;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyleContext;
import java.awt.*;
import java.sql.*;
import java.util.*;
import java.util.List;

import static java.lang.System.out;

public class GUI extends JFrame {
	private static Connection connection;

	private JPanel rootPanel;
	private JTextField urlField;
	private JTextField schemeField;
	private JTextField usernameField;
	private JPasswordField passwordField;
	private JButton connectButton;
	private JTextArea inputArea;
	private JButton executeButton;
	private JLabel connectStatusLabel;
	private JTextPane outputPane;
	private JScrollPane scrollPane;
	private JTable resultTable;

	private String dbUrl;
	private String dbUserName;
	private String dbPassword;

	private Set<String> cachedTableNames;

	public GUI() {
		connectStatusLabel.setText("<html><font color='red'>Not connected!</font></html>");
		setContentPane(rootPanel);
		setVisible(true);
		setSize(800, 720);
		setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

		usernameField.setText(System.getenv("DB_USERNAME"));
		passwordField.setText(System.getenv("DB_PASSWORD"));

		connectButton.addActionListener(event -> {
			if (connection == null)
				try {
					registerDriver();
					dbUrl = "jdbc:postgresql://" + urlField.getText() + "/" + schemeField.getText();
					dbUserName = usernameField.getText();
					dbPassword = new String(passwordField.getPassword());
					connection = DriverManager.getConnection(dbUrl, dbUserName, dbPassword);
					log("Successfully connected to " + dbUrl);
					connectStatusLabel.setText("<html><font color='green'>Connected!</font></html>");
					connectButton.setText("Disconnect");
				} catch (SQLException e) {
					logError(e.getMessage());
					connection = null;
				}
			else
				try {
					disconnect();
					connectButton.setText("Connect");
					connectStatusLabel.setText("<html><font color='red'>Not connected!</font></html>");
				} catch (SQLException e) {
					logError(e.getMessage());
				}
		});

		executeButton.addActionListener(event -> {
			String query = inputArea.getText();
			if (query.toUpperCase().contains("SELECT")) {
				performQueryWithReturn(query);
			} else {
				performQuery(query);
			}
		});
	}

	private void performQueryWithReturn(String query) {
		try (Statement statement = connection.createStatement()) {
			ResultSet result = statement.executeQuery(query);
			Set<String> columnNames = new HashSet<>();
			getTableNames().stream()
					.filter(query::contains)
					.findAny().ifPresentOrElse(name -> columnNames.addAll(getTableStructure(name).keySet()),
					() -> logError("Cannot identify table!"));

			String[] tableHeaders = columnNames.toArray(new String[]{});
			List<Set<String>> data = new ArrayList<>();
			if (!columnNames.isEmpty())
				while (result.next()) {
					Set<String> row = new LinkedHashSet<>();
					columnNames.forEach(col -> {
						try {
							row.add(result.getString(col));
						} catch (SQLException e) {
							e.printStackTrace();
						}
					});
					data.add(row);
				}
			DefaultTableModel model = new DefaultTableModel();
			model.setColumnIdentifiers(tableHeaders);
			out.println(Arrays.toString(tableHeaders));
			data.forEach(row -> model.addRow(row.toArray(new String[]{})));
			resultTable.setModel(model);
		} catch (SQLException e) {
			logError(e.getMessage());
		}
	}

	private void performQuery(String query) {
		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
			log("Updated: " + statement.getUpdateCount() + " row(s).");
		} catch (SQLException e) {
			logError(e.getMessage());
		}
	}

	private void logError(Object msg) {
		printOutput(msg, Color.RED);
	}

	private void log(Object msg) {
		printOutput(msg, Color.BLACK);
	}

	private void printOutput(Object msg, Color color) {
		outputPane.setText("");
		StyleContext sc = StyleContext.getDefaultStyleContext();
		AttributeSet aset = sc.addAttribute(SimpleAttributeSet.EMPTY, StyleConstants.Foreground, color);

		aset = sc.addAttribute(aset, StyleConstants.FontFamily, "Lucida Console");
		aset = sc.addAttribute(aset, StyleConstants.Alignment, StyleConstants.ALIGN_JUSTIFIED);

		int len = outputPane.getDocument().getLength();
		outputPane.setCaretPosition(len);
		outputPane.setCharacterAttributes(aset, false);
		outputPane.replaceSelection(String.valueOf(msg));
	}

	private static void checkConnection() {
		if (connection == null)
			throw new IllegalStateException("Not connected to database. You need to connect first!");
	}

	private void disconnect() throws SQLException {
		connection.close();
		connection = null;
		cachedTableNames = null;
	}

	private static void registerDriver() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
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

	private Set<String> getTableNames() throws SQLException {
		if (cachedTableNames != null && !cachedTableNames.isEmpty())
			return cachedTableNames;

		checkConnection();
		try (Statement statement = connection.createStatement()) {
			final String query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
			ResultSet result = statement.executeQuery(query);
			cachedTableNames = new HashSet<>() {{
				while (result.next())
					add(result.getString("tablename"));
			}};
			return cachedTableNames;
		}
	}
}
