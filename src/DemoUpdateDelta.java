import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DemoUpdateDelta {
	static CyclicBarrier barrier = new CyclicBarrier(2);

	public static HikariDataSource buildDataSource() throws Exception {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:mysql://localhost:3306/deposit-demo");
		config.setUsername("root");
		config.setPassword("123qwe");
		HikariDataSource ds = new HikariDataSource(config);
		return ds;
	}

	public static void deposit(HikariDataSource ds, long userid, BigDecimal amount, boolean doSync)
			throws SQLException {
		try (Connection dbconn = ds.getConnection()) {
			dbconn.setAutoCommit(false);
			if (doSync) {

				try {
					barrier.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (BrokenBarrierException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			PreparedStatement stmt = dbconn.prepareStatement("update account set balance = balance +?  where id = ?; ");
			stmt.setBigDecimal(1, amount);
			stmt.setLong(2, userid);
			stmt.executeUpdate();
			System.out.println("update set balance = balance +?  where id = ?; ");

			dbconn.commit();
		}
	}

	public static void main(String[] args) throws Exception {
		HikariDataSource ds = buildDataSource();

		boolean concurrent = true;
		if (!concurrent) {
			deposit(ds, 1, new BigDecimal(10.00), false);
		} else {
			ExecutorService exe = Executors.newFixedThreadPool(2);
			exe.execute(new Runnable() {
				@Override
				public void run() {
					try {
						deposit(ds, 1, new BigDecimal(10.00), true);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			});
			exe.execute(new Runnable() {

				@Override
				public void run() {
					try {
						deposit(ds, 1, new BigDecimal(10.00), true);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			});
			exe.shutdown();
			exe.awaitTermination(10, TimeUnit.SECONDS);
		}

		return;
	}
}
