package de.diddiz.LogBlock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import org.bukkit.ChatColor;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;

public class Rollback implements Runnable
{
	private LinkedBlockingQueue<Edit> edits = new LinkedBlockingQueue<Edit>();
	PreparedStatement ps = null;
	private Player player;
	private Connection conn = null;
    private boolean redo = false;

	Rollback(Player player, Connection conn, String name, int minutes, String table) {
		this.player = player;
		this.conn = conn;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement("SELECT type, data, replaced, x, y, z FROM `" + table + "` INNER JOIN `lb-players` USING (`playerid`) WHERE playername = ? AND date > date_sub(now(), INTERVAL ? MINUTE) ORDER BY date DESC", Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, name);
			ps.setInt(2, minutes);
		} catch (SQLException ex) {
			LogBlock.log.log(Level.SEVERE, this.getClass().getName() + " SQL exception", ex);
			player.sendMessage(ChatColor.RED + "Error, check server logs.");
			return;
		}
	}
	
	Rollback(Player player, Connection conn, String name, int radius, int minutes, String table) {
		this.player = player;
		this.conn = conn;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement("SELECT type, data, replaced, x, y, z FROM `" + table + "` INNER JOIN `lb-players` USING (`playerid`) WHERE playername = ? AND x > ? AND x < ? AND z > ? AND z < ? AND date > date_sub(now(), INTERVAL ? MINUTE) ORDER BY date DESC", Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, name);
			ps.setInt(2, player.getLocation().getBlockX()-radius);
			ps.setInt(3, player.getLocation().getBlockX()+radius);
			ps.setInt(4, player.getLocation().getBlockZ()-radius);
			ps.setInt(5, player.getLocation().getBlockZ()+radius);
			ps.setInt(6, minutes);
		} catch (SQLException ex) {
			LogBlock.log.log(Level.SEVERE, this.getClass().getName() + " SQL exception", ex);
			player.sendMessage(ChatColor.RED + "Error, check server logs.");
			return;
		}
	}

	Rollback(Player player, Connection conn, int radius, int minutes, String table) {
		this.player = player;
		this.conn = conn;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement("SELECT type, data, replaced, x, y, z FROM `" + table + "` WHERE x > ? AND x < ? AND z > ? AND z < ? AND date > date_sub(now(), INTERVAL ? MINUTE) ORDER BY date DESC", Statement.RETURN_GENERATED_KEYS);
			ps.setInt(1, player.getLocation().getBlockX()-radius);
			ps.setInt(2, player.getLocation().getBlockX()+radius);
			ps.setInt(3, player.getLocation().getBlockZ()-radius);
			ps.setInt(4, player.getLocation().getBlockZ()+radius);
			ps.setInt(5, minutes);
		} catch (SQLException ex) {
			LogBlock.log.log(Level.SEVERE, this.getClass().getName() + " SQL exception", ex);
			player.sendMessage(ChatColor.RED + "Error, check server logs.");
			return;
		}
	}
	
	Rollback(Player player, Connection conn, Location loc1, Location loc2, int minutes, String table) {
		this.player = player;
		this.conn = conn;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement("SELECT type, data, replaced, x, y, z FROM `" + table + "` WHERE x >= ? AND x <= ? AND y >= ? AND y <= ? AND z >= ? AND z <= ? AND date > date_sub(now(), INTERVAL ? MINUTE) ORDER BY date DESC", Statement.RETURN_GENERATED_KEYS);
			ps.setInt(1, Math.min(loc1.getBlockX(), loc2.getBlockX()));
			ps.setInt(2, Math.max(loc1.getBlockX(), loc2.getBlockX()));
			ps.setInt(3, Math.min(loc1.getBlockY(), loc2.getBlockY()));
			ps.setInt(4, Math.max(loc1.getBlockY(), loc2.getBlockY()));
			ps.setInt(5, Math.min(loc1.getBlockZ(), loc2.getBlockZ()));
			ps.setInt(6, Math.max(loc1.getBlockZ(), loc2.getBlockZ()));
			ps.setInt(7, minutes);
		} catch (SQLException ex) {
			LogBlock.log.log(Level.SEVERE, this.getClass().getName() + " SQL exception", ex);
			player.sendMessage(ChatColor.RED + "Error, check server logs.");
			return;
		}
	}

    Rollback(Player player, Connection conn, Location loc1, Location loc2, boolean redo, int minutes, String table) {
		this.player = player;
		this.conn = conn;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement("SELECT type, data, replaced, x, y, z FROM `" + table + "` WHERE x >= ? AND x <= ? AND y >= ? AND y <= ? AND z >= ? AND z <= ? AND date > date_sub(now(), INTERVAL ? MINUTE) ORDER BY date "+(redo?"ASC":"DESC"), Statement.RETURN_GENERATED_KEYS);
			ps.setInt(1, Math.min(loc1.getBlockX(), loc2.getBlockX()));
			ps.setInt(2, Math.max(loc1.getBlockX(), loc2.getBlockX()));
			ps.setInt(3, Math.min(loc1.getBlockY(), loc2.getBlockY()));
			ps.setInt(4, Math.max(loc1.getBlockY(), loc2.getBlockY()));
			ps.setInt(5, Math.min(loc1.getBlockZ(), loc2.getBlockZ()));
			ps.setInt(6, Math.max(loc1.getBlockZ(), loc2.getBlockZ()));
			ps.setInt(7, minutes);
            this.redo = redo;
		} catch (SQLException ex) {
			LogBlock.log.log(Level.SEVERE, this.getClass().getName() + " SQL exception", ex);
			player.sendMessage(ChatColor.RED + "Error, check server logs.");
			return;
		}
	}
	
	public void run() {
		ResultSet rs = null;
		edits.clear();
		try {
			rs = ps.executeQuery();
			while (rs.next())
            {
                Edit e;
                if(!this.redo)
				    e = new Edit(rs.getInt("type"), rs.getInt("replaced"), rs.getByte("data"), rs.getInt("x"), rs.getInt("y"), rs.getInt("z"), player.getWorld());
                else
                    e = new Edit(rs.getInt("replaced"), rs.getInt("type"), rs.getByte("data"), rs.getInt("x"), rs.getInt("y"), rs.getInt("z"), player.getWorld(), true);
				edits.offer(e);
			}
		} catch (SQLException ex) {
			LogBlock.log.log(Level.SEVERE, this.getClass().getName() + " SQL exception", ex);
			player.sendMessage("§cError, check server logs.");
			return;
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException ex) {
				LogBlock.log.log(Level.SEVERE, this.getClass().getName() + " SQL exception on close", ex);
				player.sendMessage("§cError, check server logs.");
				return;
			}
		}
		int changes = edits.size();
		int rolledBack = 0;
		player.sendMessage(ChatColor.GREEN + "" + changes + " Changes found.");
		long start = System.currentTimeMillis();
		Edit e = edits.poll();
		while (e != null)
		{
			if (e.perform())
				rolledBack++;
			e = edits.poll();
		}
		player.sendMessage(ChatColor.GREEN + "Rollback finished successfully");
		player.sendMessage(ChatColor.GREEN + "Undid " + rolledBack + " of " + changes + " changes");
		player.sendMessage(ChatColor.GREEN + "Took:  " + (System.currentTimeMillis() - start) + "ms");
	}

	private class Edit
	{
		int type, replaced;
		int x, y, z;
		byte data;
		World world;
        boolean force;
		
		Edit(int type, int replaced, byte data, int x, int y, int z, World world) {
			this.type = type;
			this.replaced = replaced;
			this.data = data;
			this.x = x;
			this.y = y;
			this.z = z;
			this.world = world;
		}

        Edit(int type, int replaced, byte data, int x, int y, int z, World world, boolean force) {
			this.type = type;
			this.replaced = replaced;
			this.data = data;
			this.x = x;
			this.y = y;
			this.z = z;
			this.world = world;
            this.force = force;
		}
		
		public boolean perform() {
			if (type > 0 && type == replaced)
				return false;
			try {
				Block block = world.getBlockAt(x, y, z);
				if (!world.isChunkLoaded(block.getChunk()))
					world.loadChunk(block.getChunk());
				if(force)
                    return block.setTypeIdAndData(replaced, data, false);
                else if (block.getTypeId() == type || (block.getTypeId() >= 8 && block.getTypeId() <= 11) || block.getTypeId() == 51 || (type == 0 && replaced == 0))
					return block.setTypeIdAndData(replaced, data, false);
			} catch (Exception ex) {
					LogBlock.log.severe("[LogBlock Rollback] " + ex.toString());
			}
			return false;
		}
	}
}