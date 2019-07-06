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

public class DemoReadModifySave {
	static CyclicBarrier barrier = new CyclicBarrier(2);
	public static HikariDataSource buildDataSource() throws Exception {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:mysql://localhost:3306/deposit-demo");
		config.setUsername("root");
		config.setPassword("123qwe");
		HikariDataSource ds = new HikariDataSource(config);
		return ds;
	}
	
	public static void deposit(HikariDataSource ds, long userid, BigDecimal amount, boolean doSync) throws SQLException {
		try (Connection dbconn = ds.getConnection()) {
			dbconn.setAutoCommit(false);
			PreparedStatement stmt = dbconn.prepareStatement("select balance from account where id = ?; ");
			stmt.setLong(1, userid);
			ResultSet rs = stmt.executeQuery();
			boolean next = rs.next();
			BigDecimal oldBalance = rs.getBigDecimal(1); // oldBalance
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
			
			System.out.println("update account set balance = ? where id = ?; ");
			PreparedStatement update = dbconn.prepareStatement("update account set balance = ? where id = ?; ");
			update.setBigDecimal(1, oldBalance.add(amount));
			update.setLong(2, userid);
			update.executeUpdate();
			dbconn.commit();
		}
	}
	
	public static void main(String[] args) throws Exception {
		HikariDataSource ds = buildDataSource();
		
		boolean concurrent = true;
		if (!concurrent) {
			deposit( ds, 1, new BigDecimal(10.00), false);	
		} else {
			ExecutorService exe = Executors.newFixedThreadPool(2);
			exe.execute(new Runnable() {
				@Override
				public void run() {
					try {
						deposit( ds, 1, new BigDecimal(10.00), true);
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
						deposit( ds, 1, new BigDecimal(10.00), true);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
				
			});
			exe.shutdown();
			exe.awaitTermination(10, TimeUnit.SECONDS);
		}
		
		return ;
	}
}
