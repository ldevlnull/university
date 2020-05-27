package lab9;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.text.AttributeSet;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyleContext;
import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.List;

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
	private JButton exportQueryButton;
	private JCheckBox includeHeadersCheckBox;

	private String dbUrl;
	private String dbUserName;
	private String dbPassword;

	private List<Set<String>> lastSelectQueryCache;

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
			checkConnection();
			String query = inputArea.getText();
			if (query.toUpperCase().contains("SELECT")) {
				performQueryWithReturn(query);
			} else {
				performQuery(query);
			}
		});

		exportQueryButton.addActionListener(event -> exportToCSV());
	}

	private void exportToCSV() {
		if(lastSelectQueryCache == null || lastSelectQueryCache.isEmpty()) {
			logError("Error during export: nothing to save. Make a SELECT query first!");
			return;
		}

		JFileChooser chooser = new JFileChooser();
		chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
		chooser.setApproveButtonText("Save");
		int res = chooser.showOpenDialog(rootPane);
		if (res == JFileChooser.APPROVE_OPTION) {
			File path = chooser.getSelectedFile();
			if (!path.getName().substring(path.getName().length() - 3).equalsIgnoreCase("csv"))
				path = new File(path.toString() + ".csv");
			try (PrintWriter writer = new PrintWriter(new FileWriter(path))) {
				if (includeHeadersCheckBox.isSelected()) {
					lastSelectQueryCache.stream().skip(1).forEach(values -> writer.println(values.toString().replaceAll("[\\[\\]]", "")));
					log(lastSelectQueryCache.size()-1 + " row(s) exported to " + path);
				} else {
					lastSelectQueryCache.forEach(values -> writer.println(values.toString().replaceAll("[\\[\\]]", "")));
					log(lastSelectQueryCache.size() + " row(s) exported to " + path);
				}
			} catch (IOException io) {
				logError(io.getMessage());
			}
		}
	}

	private void performQueryWithReturn(String query) {
		try (Statement statement = connection.createStatement()) {
			ResultSet result = statement.executeQuery(query);
			Set<String> columnNames = new HashSet<>();
			for (int i = 1; i <= result.getMetaData().getColumnCount(); i++)
				columnNames.add(result.getMetaData().getColumnName(i));

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
			data.forEach(row -> model.addRow(row.toArray(new String[]{})));
			resultTable.setModel(model);
			lastSelectQueryCache = data;
			log("Retrieved " + data.size() + " row(s).");
			lastSelectQueryCache.add(0, columnNames);
		} catch (SQLException e) {
			logError(e.getMessage());
		}
	}

	private void performQuery(String query) {
		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
			if (statement.getUpdateCount() != 0)
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
	}

	private static void registerDriver() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
