package de.diddiz.LogBlock;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sk89q.worldedit.Vector;
import com.sk89q.worldedit.blocks.ItemType;
import com.sk89q.worldguard.LocalPlayer;
import com.sk89q.worldguard.bukkit.BukkitPlayer;
import com.sk89q.worldguard.bukkit.WorldGuardPlugin;
import com.sk89q.worldguard.protection.ApplicableRegionSet;
import com.sk89q.worldguard.protection.regionmanager.GlobalRegionManager;
import com.sk89q.worldguard.protection.regionmanager.RegionManager;
import org.bukkit.ChatColor;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.Event;
import org.bukkit.event.Event.Type;
import org.bukkit.event.block.*;
import org.bukkit.event.entity.EntityExplodeEvent;
import org.bukkit.event.entity.EntityListener;
import org.bukkit.event.player.PlayerEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerListener;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.PluginManager;
import org.bukkit.plugin.java.JavaPlugin;

import bootswithdefer.JDCBPool.JDCConnectionDriver;

import com.nijikokun.bukkit.Permissions.Permissions;
import com.sk89q.worldedit.bukkit.WorldEditPlugin;
import com.sk89q.worldedit.bukkit.selections.CuboidSelection;
import com.sk89q.worldedit.bukkit.selections.Selection;

public class LogBlock extends JavaPlugin
{
	static Logger log;
    static Config config;
	private Consumer consumer = null;
	private LinkedBlockingQueue<BlockRow> bqueue = new LinkedBlockingQueue<BlockRow>();
    private Map<Player, Integer> lookupToolMap;
    private Map<Player, Integer> lookupBlockMap;
    private static WorldGuardPlugin wg;



    protected void setupWorldGuard()
    {
        Plugin p = this.getServer().getPluginManager().getPlugin("WorldGuard");
        if(wg==null)
            if(p!=null)
                wg = (WorldGuardPlugin) p;
    }

    public static WorldGuardPlugin getWg() {
        return wg;
    }
	
	@Override
	public void onEnable() {
		log = getServer().getLogger();
		try	{
			config = new Config(getConfiguration());
			if (config.usePermissions)	{
				if (getServer().getPluginManager().getPlugin("Permissions") != null) 
					log.info("[LogBlock] Permissions enabled");
				else {
					config.usePermissions = false;
					log.warning("[LogBlock] Permissions plugin not found. Using default permissions.");
				}
			}
			new JDCConnectionDriver(config.dbDriver, config.dbUrl, config.dbUsername, config.dbPassword);
			Connection conn = getConnection();
			conn.close();
            setupWorldGuard();
		} catch (Exception ex) {
			log.log(Level.SEVERE, "[LogBlock] Exception while enabling", ex);
			getServer().getPluginManager().disablePlugin(this);
			return;
		}
		if (config.worldNames == null || config.worldTables == null || config.worldNames.size() == 0 || config.worldNames.size() != config.worldTables.size()) {
			log.log(Level.SEVERE, "[LogBlock] worldNames or worldTables not set porperly");
			getServer().getPluginManager().disablePlugin(this);
			return;
		}
		if (!checkTables()) {
			log.log(Level.SEVERE, "[LogBlock] Errors while checking tables. They may not exist.");
			getServer().getPluginManager().disablePlugin(this);
			return;
		}
		if (config.keepLogDays >= 0)
			new Thread(new ClearLog(getConnection())).start();
        lookupToolMap = new HashMap<Player, Integer>();
        lookupBlockMap = new HashMap<Player, Integer>();
		LBBlockListener lbBlockListener = new LBBlockListener();
		LBPlayerListener lbPlayerListener = new LBPlayerListener();
		PluginManager pm = getServer().getPluginManager();
		// pm.registerEvent(Type.PLAYER_ITEM, lbPlayerListener, Event.Priority.Monitor, this);
		pm.registerEvent(Type.PLAYER_JOIN, lbPlayerListener, Event.Priority.Normal, this);
		// pm.registerEvent(Type.BLOCK_RIGHTCLICKED, lbBlockListener, Event.Priority.Monitor, this);
		pm.registerEvent(Type.BLOCK_PLACE, lbBlockListener, Event.Priority.Monitor, this);
		pm.registerEvent(Type.BLOCK_BREAK, lbBlockListener, Event.Priority.Monitor, this);
		pm.registerEvent(Type.SIGN_CHANGE, lbBlockListener, Event.Priority.Monitor, this);
		if (config.logFire)
			pm.registerEvent(Type.BLOCK_BURN, lbBlockListener, Event.Priority.Monitor, this);
		if (config.logExplosions)
			pm.registerEvent(Type.ENTITY_EXPLODE, new LBEntityListener(), Event.Priority.Monitor, this);
		// if (config.logChestAccess)
			pm.registerEvent(Type.PLAYER_INTERACT, lbPlayerListener, Event.Priority.Monitor, this);
		if (config.logLeavesDecay)
			pm.registerEvent(Type.LEAVES_DECAY, lbBlockListener, Event.Priority.Monitor, this);
		consumer = new Consumer();
		new Thread(consumer).start();
		log.info("Logblock v" + getDescription().getVersion() + " enabled.");
	}

	@Override
	public void onDisable() {
		if (consumer != null) {
			log.info("[LogBlock] Stopping consumer");
			consumer.stop();
		}
		log.info("LogBlock disabled.");
	}
	
	@Override
	public boolean onCommand(CommandSender sender, Command cmd, String commandLabel, String[] args)	{
		if (cmd.getName().equalsIgnoreCase("lb")) {
		if (!(sender instanceof Player)) {
			sender.sendMessage("You aren't a player");
			return true;
		}
		Player player = (Player)sender;
		Connection conn = getConnection();
		String table = getTable(player);
		if (conn == null) {
			player.sendMessage(ChatColor.RED + "Can't create SQL connection.");
			return true;
		} else if (table == null) {
			player.sendMessage(ChatColor.RED + "This world isn't logged");
			return true;
		}
		if (args.length == 0) {
			player.sendMessage(ChatColor.LIGHT_PURPLE + "LogBlock v" + getDescription().getVersion() + " by DiddiZ");
			player.sendMessage(ChatColor.LIGHT_PURPLE + "Type /lb help for help");
		} else if (args[0].equalsIgnoreCase("area")) {
			if (CheckPermission(player,"logblock.area")) {
				int radius = config.defaultDist;
				if (args.length == 2 && isInt(args[1]))
					radius = Integer.parseInt(args[1]);
				new Thread(new AreaStats(conn, player, radius, table)).start();
			} else
				player.sendMessage(ChatColor.RED + "You aren't allowed to do this");
		} else if (args[0].equalsIgnoreCase("world")) {
			if (CheckPermission(player,"logblock.area")) {
				new Thread(new AreaStats(conn, player, Short.MAX_VALUE, table)).start();
			} else
				player.sendMessage(ChatColor.RED + "You aren't allowed to do this");
		} else if (args[0].equalsIgnoreCase("player")) {
			if (CheckPermission(player,"logblock.area")) {
				if (args.length == 2 || args.length == 3) {
					int radius = config.defaultDist;
					if (args.length == 3 && isInt(args[2]))
						radius = Integer.parseInt(args[2]);
					new Thread(new PlayerAreaStats(conn, player, args[1], radius, table)).start();
					} else
						player.sendMessage(ChatColor.RED + "Usage: /lb player [name] <radius>"); 
			} else
				player.sendMessage(ChatColor.RED + "You aren't allowed to do this");
		} else if (args[0].equalsIgnoreCase("block")) {
			if (CheckPermission(player,"logblock.area")) {
				if (args.length == 2 || args.length == 3) {
					Material mat = Material.matchMaterial(args[1]);
					int radius = config.defaultDist;
					if (args.length == 3 && isInt(args[2]))
						radius = Integer.parseInt(args[2]);
					if (mat != null)
						new Thread(new AreaBlockSearch(conn, player, mat.getId(), radius, table)).start();
					else
						player.sendMessage(ChatColor.RED + "Can't find any item like '" + args[1] + "'");
				} else
					player.sendMessage(ChatColor.RED + "Usage: /lb block [type] <radius>");
			} else
				player.sendMessage(ChatColor.RED + "You aren't allowed to do this");
		} else if (args[0].equalsIgnoreCase("rollback")) {
			if (CheckPermission(player,"logblock.rollback")) {
				if (args.length >= 2) {
					int minutes = config.defaultTime;
					if (args[1].equalsIgnoreCase("player")) {
						if (args.length == 3 || args.length == 5) {
							if (args.length == 5)
								minutes = parseTimeSpec(args[3], args[4]);
							player.sendMessage(ChatColor.GREEN + "Rolling back " + args[2] + " by " + minutes + " minutes.");
							new Thread(new Rollback(player, conn, args[2], minutes, table)).start();
						} else 
							player.sendMessage(ChatColor.RED + "Usage: /lb rollback player [name] <time> <minutes|hours|days>");
					} else if (args[1].equalsIgnoreCase("area")) {
						if (args.length == 3 || args.length == 5) {
							if (args.length == 5)
								minutes = parseTimeSpec(args[3], args[4]);
							if (isInt(args[2])) {
								player.sendMessage(ChatColor.GREEN + "Rolling back area within " + args[2] + " blocks of you by " + minutes + " minutes.");
								getServer().getScheduler().scheduleSyncDelayedTask(this, new Rollback(player, conn, Integer.parseInt(args[2]), minutes, table));
							} else
								player.sendMessage(ChatColor.RED + "Can't parse to an int: " + args[2]);
						} else
							player.sendMessage(ChatColor.RED + "Usage /lb rollback area [radius] <time> <minutes|hours|days>");
					} else if (args[1].equalsIgnoreCase("playerarea")) {
						if (args.length == 4 || args.length == 6) {
							if (args.length == 6)
								minutes = parseTimeSpec(args[4], args[5]);
							if (isInt(args[3])) {
								player.sendMessage(ChatColor.GREEN + "Rolling back " + args[2] + " within " + args[3] + " blocks by " + minutes + " minutes.");
								getServer().getScheduler().scheduleSyncDelayedTask(this, new Rollback(player, conn, args[2], Integer.parseInt(args[3]), minutes, table));
							} else
								player.sendMessage(ChatColor.RED + "Can't parse to an int: " + args[3]);
						} else
							player.sendMessage(ChatColor.RED + "Usage: /lb rollback playerarea [player] [radius] <time> <minutes|hours|days>");
					} else if (args[1].equalsIgnoreCase("selection")) {
						if (args.length == 2 || args.length == 4 || args.length == 5) {
                            boolean redo = false;
							if (args.length > 3)
								minutes = parseTimeSpec(args[2], args[3]);
                            if(args.length == 5)
                                redo = args[4].equalsIgnoreCase("redo");
							Plugin we = getServer().getPluginManager().getPlugin("WorldEdit");
							if (we != null) {
								Selection sel = ((WorldEditPlugin)we).getSelection(player);
								if (sel != null) {
									if (sel instanceof CuboidSelection) {
                                        if(!redo)
                                        {
                                            player.sendMessage(ChatColor.GREEN + "Rolling back selection by " + minutes + " minutes.");
                                            getServer().getScheduler().scheduleSyncDelayedTask(this, new Rollback(player, conn, sel.getMinimumPoint(), sel.getMaximumPoint(), minutes, table));
                                        } else {
                                            player.sendMessage(ChatColor.GREEN + "Redo-ing selection since " + minutes + " minutes.");
                                            getServer().getScheduler().scheduleSyncDelayedTask(this, new Rollback(player, conn, sel.getMinimumPoint(), sel.getMaximumPoint(), true, minutes, table));
                                        }
									} else
										player.sendMessage(ChatColor.RED + "You have to define a cuboid selection");
								} else
									player.sendMessage(ChatColor.RED + "No selection defined");
							} else
								player.sendMessage(ChatColor.RED + "WorldEdit plugin not found");
						} else 
							player.sendMessage(ChatColor.RED + "Usage: /lb rollback selection <time> <minutes|hours|days> <redo>");
					} else
						player.sendMessage(ChatColor.RED + "Wrong rollback mode");
				} else {
					player.sendMessage(ChatColor.RED + "Usage: ");
					player.sendMessage(ChatColor.RED + "/lb rollback player [name] <time> <minutes|hours|days>");
					player.sendMessage(ChatColor.RED + "/lb rollback area [radius] <time> <minutes|hours|days>");
					player.sendMessage(ChatColor.RED + "/lb rollback playerarea [name] [radius] <time> <minutes|hours|days>");
					player.sendMessage(ChatColor.RED + "/lb rollback selection <time> <minutes|hours|days>");
				}
			} else
				player.sendMessage(ChatColor.RED + "You aren't allowed to do this");
		} else if (args[0].equalsIgnoreCase("writelogfile")) {
			if (CheckPermission(player,"logblock.rollback")) {
				if (args.length == 2) {
					new Thread(new WriteLogFile(conn, player, args[1], table)).start();
				}
				else
					player.sendMessage(ChatColor.RED + "Usage: /lb writelogfile [name]");
			} else
				player.sendMessage(ChatColor.RED + "You aren't allowed to do this");
		} else if (args[0].equalsIgnoreCase("me")) {
			if (CheckPermission(player,"logblock.me")) {
				new Thread(new PlayerAreaStats(conn, player, player.getName(), Short.MAX_VALUE, table)).start();
			} else
				player.sendMessage(ChatColor.RED + "You aren't allowed to do this");
		} else if (args[0].equalsIgnoreCase("help")) {
			player.sendMessage(ChatColor.LIGHT_PURPLE + "LogBlock Commands:");
			player.sendMessage(ChatColor.LIGHT_PURPLE + "/lb area <radius>");
			player.sendMessage(ChatColor.LIGHT_PURPLE + "/lb world");
			player.sendMessage(ChatColor.LIGHT_PURPLE + "/lb player [name] <radius>");
			player.sendMessage(ChatColor.LIGHT_PURPLE + "/lb block [type] <radius>");
			player.sendMessage(ChatColor.LIGHT_PURPLE + "/lb rollback [rollback mode]");
		} else
			player.sendMessage(ChatColor.RED + "Wrong argument. Type /lb help for help");
		return true;
        } else if(cmd.getName().equals("lt")) {
            if (!(sender instanceof Player)) {
                sender.sendMessage("You aren't a player");
                return true;
            }
            Player player = (Player)sender;
            assignTool(player, player.getItemInHand().getTypeId());
            return true;
        } else if(cmd.getName().equalsIgnoreCase("ltb")) {
            if (!(sender instanceof Player)) {
                sender.sendMessage("You aren't a player");
                return true;
            }
            Player player = (Player)sender;
            assignBlock(player, player.getItemInHand().getTypeId());
            return true;
        }
        return false;
	}

    public void assignTool(Player player, Integer item)
    {
        if (item > 0 && item < 255) {
            player.sendMessage(ChatColor.GOLD + "Can't bind to "+ ItemType.toName(item)+". Can't use blocks!");
        } else if (item == 263 || item == 348) {
            player.sendMessage(ChatColor.GOLD + "Can't bind to "+ItemType.toName(item)+". Item is nog usable!");
        } else {
            player.sendMessage(ChatColor.GOLD + "Lookup tool bound to "+ItemType.toName(item)+".");
            lookupToolMap.put(player, item);
        }
    }

    public int getAssignedTool(Player player)
    {
        if(lookupToolMap.containsKey(player))
        {
            return lookupToolMap.get(player);
        } else {
            return config.toolID;
        }
    }

    public void assignBlock(Player player, Integer blockId)
    {
        if (blockId > 0 && blockId < 255) {
            lookupBlockMap.put(player, blockId);
            // player.sendMessage(ChatColor.GOLD + "Can't bind to "+ItemType.toName(blockId)+". Can't use blocks!");
            player.sendMessage(ChatColor.GOLD + "Lookup block bound to "+ItemType.toName(blockId)+".");
        } else {
            player.sendMessage(ChatColor.GOLD + "Can't bind to "+ItemType.toName(blockId)+". Can't use items!");
        }
    }

    public int getAssignedBlock(Player player)
    {
        if(lookupBlockMap.containsKey(player))
        {
            return lookupBlockMap.get(player);
        } else {
            return config.toolblockID;
        }
    }

	private Connection getConnection()
	{
		try {
			return DriverManager.getConnection("jdbc:jdc:jdcpool");
		} catch (SQLException ex) {
			log.log(Level.SEVERE, "[LogBlock] SQL exception", ex);
			return null;
		}
	}
	
	private boolean checkTables() {
		Connection conn = getConnection();
		Statement state = null;
		if (conn == null)
			return false;
		try {
			DatabaseMetaData dbm = conn.getMetaData();
			state = conn.createStatement();
			if (!dbm.getTables(null, null, "lb-players", null).next())	{
				log.log(Level.INFO, "[LogBlock] Crating table players.");
				state.execute("CREATE TABLE `lb-players` (`playerid` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT, `playername` varchar(32) NOT NULL DEFAULT '-', PRIMARY KEY (`playerid`), UNIQUE (`playername`))");
				if (!dbm.getTables(null, null, "lb-players", null).next())
					return false;
			}
			state.execute("INSERT IGNORE INTO `lb-players` (`playername`) VALUES ('" + config.logTNTExplosionsAs + "'), ('" + config.logCreeperExplosionsAs + "'), ('" + config.logFireAs + "'), ('" + config.logLeavesDecayAs + "')");
			for (int i = 0; i < config.worldNames.size(); i++) {
				String table = config.worldTables.get(i);
				if (!dbm.getTables(null, null, table, null).next())	{
					log.log(Level.INFO, "[LogBlock] Crating table " + table + ".");
					state.execute("CREATE TABLE `" + table + "` (`id` INT NOT NULL AUTO_INCREMENT, `date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00', `playerid` SMALLINT UNSIGNED NOT NULL DEFAULT '0', `replaced` TINYINT UNSIGNED NOT NULL DEFAULT '0', `type` TINYINT UNSIGNED NOT NULL DEFAULT '0', `data` TINYINT UNSIGNED NOT NULL DEFAULT '0', `x` SMALLINT NOT NULL DEFAULT '0', `y` TINYINT UNSIGNED NOT NULL DEFAULT '0',`z` SMALLINT NOT NULL DEFAULT '0', PRIMARY KEY (`id`), KEY `coords` (`y`,`x`,`z`), KEY `type` (`type`), KEY `data` (`data`), KEY `replaced` (`replaced`));");
					if (!dbm.getTables(null, null, table, null).next())
						return false;
				}
				if (!dbm.getTables(null, null, table + "-sign", null).next()) {
					log.log(Level.INFO, "[LogBlock] Crating table " + table + "-sign.");
					state.execute("CREATE TABLE `" + table + "-sign` (`id` INT NOT NULL, `signtext` TEXT, PRIMARY KEY (`id`));");
					if (!dbm.getTables(null, null, table + "-sign", null).next())
						return false;
				}
				if (!dbm.getTables(null, null, table + "-chest", null).next()) {
					log.log(Level.INFO, "[LogBlock] Crating table " + table + "-chest.");
					state.execute("CREATE TABLE `" + table + "-chest` (`id` INT NOT NULL, `intype` SMALLINT UNSIGNED NOT NULL DEFAULT '0', `inamount` TINYINT UNSIGNED NOT NULL DEFAULT '0', `outtype` SMALLINT UNSIGNED NOT NULL DEFAULT '0', `outamount` TINYINT UNSIGNED NOT NULL DEFAULT '0', PRIMARY KEY (`id`));");
					if (!dbm.getTables(null, null, table + "-chest", null).next())
						return false;
				}
			}
			return true;
		} catch (SQLException ex) {
			log.log(Level.SEVERE, "[LogBlock] SQL exception while checking tables", ex);
		} finally {
			try {
				if (state != null)
					state.close();
				if (conn != null)
					conn.close();
			} catch (SQLException ex) {
				log.log(Level.SEVERE, "[LogBlock] SQL exception on close", ex);
			}
		}
		return false;
	}
	
	private String getTable (Player player) {
		return getTable(player.getWorld().getName());
	}
	
	private String getTable (Block block) {
		return getTable(block.getWorld().getName());
	}
	
	private String getTable (String worldName) {
		int idx = config.worldNames.indexOf(worldName);
		if (idx == -1)
			return null;
		return config.worldTables.get(idx);
	}

	private void queueBlock(Player player, Block block, int typeAfter) {
		queueBlock(player.getName(), block, 0, typeAfter, (byte)0, null, null);
	}
	
	private void queueBlock(String playerName, Block block, int typeBefore, int typeAfter, byte data) {
		queueBlock(playerName, block, typeBefore, typeAfter, data, null, null);
	}
	
	private void queueBlock(Player player, Block block, short inType, byte inAmount, short outType, byte outAmount) {
		queueBlock(player.getName(), block, 54, 54, (byte)0, null, new ChestAccess(inType, inAmount, outType, outAmount));
	}
	
	private void queueBlock(String playerName, Block block, int typeBefore, int typeAfter, byte data, String signtext, ChestAccess ca) {
		if (block == null || typeBefore < 0 || typeAfter < 0)
			return;
		String table = getTable(block);
		if (table == null)
			return;
		if (playerName.length() > 32)
			playerName = playerName.substring(0, 31);
		BlockRow row = new BlockRow(table, playerName, typeBefore, typeAfter, data, block.getX(), block.getY(), block.getZ());
		if (signtext != null)
			row.signtext = signtext;
		if (ca != null)
			row.ca = ca;
		if (!bqueue.offer(row))
			log.info("[LogBlock] Failed to queue block for " + playerName);
	}
	
    private boolean CheckPermission(Player player, String permission) {
        if (config.usePermissions)
        {
            return Permissions.Security.permission(player, permission);
        }

        else {
            if (permission.equals("logblock.lookup"))
                return true;
            else if (permission.equals("logblock.me"))
                return true;
            else if (permission.equals("logblock.area"))
                return player.isOp();
            else if (permission.equals("logblock.rollback"))
                return player.isOp();
        }
        return false;
    }

    private boolean CheckPermissionWorldGuard(Player player, Location block) {
        boolean regionowner=false;

        if(getWg()!=null)
        {
            GlobalRegionManager gm = getWg().getGlobalRegionManager();
            RegionManager rm = gm.getRegionManager(block.getWorld().getName());
            Location loc = block.clone();
            Vector vec = new Vector(loc.getX(), loc.getY(), loc.getZ());
            LocalPlayer localplayer = new BukkitPlayer(getWg(), player);
            ApplicableRegionSet regionset = rm.getApplicableRegions(vec);
            String domainId = regionset.getAffectedRegionId();
            regionowner = regionset.isOwner(localplayer);
            if(!regionowner)
            {
                player.sendMessage(ChatColor.RED + "You aren't an owner of this region!");
            }
        }

        return regionowner;
    }
	
	static int parseTimeSpec(String timespec) {
		String[] split = timespec.split(" ");
		if (split.length != 2)
			return 0;
		return parseTimeSpec(split[0], split[1]);
	}

	static int parseTimeSpec(String time, String unit) {
		int min;
		try {
			min = Integer.parseInt(time);
		} catch (NumberFormatException ex) {
			return 0;
		}
		if (unit.startsWith("hour"))
			min *= 60;
		else if (unit.startsWith("day"))
			min *= (60*24);
		return min;
	}

	private class LBPlayerListener extends PlayerListener
	{
		public void onPlayerInteract(PlayerInteractEvent event) {
            if(event.getAction() != Action.RIGHT_CLICK_BLOCK)
                return;
            Location loc = event.getClickedBlock().getLocation();

            
            if (!event.isCancelled() && event.getClickedBlock().getType() == Material.CHEST)  {
                if (event.getPlayer().getItemInHand().getTypeId() == getAssignedTool(event.getPlayer())
                    && (CheckPermission(event.getPlayer(), "logblock.lookup") || CheckPermissionWorldGuard(event.getPlayer(), loc)))
                {
                    new Thread(new BlockStats(getConnection(), event.getPlayer(), event.getClickedBlock(), getTable(event.getClickedBlock()))).start();
	    			event.setCancelled(true);
                }
	    		else
                {
                    if (config.logChestAccess)
	    			    queueBlock(event.getPlayer(), event.getClickedBlock(), (short)0, (byte)0, (short)0, (byte)0);
                }
            } else if (!event.isCancelled() && event.getClickedBlock() != null) {
                if (event.getPlayer().getItemInHand().getTypeId()== getAssignedTool(event.getPlayer()) && (CheckPermission(event.getPlayer(), "logblock.lookup") || CheckPermissionWorldGuard(event.getPlayer(), loc)))
				    new Thread(new BlockStats(getConnection(), event.getPlayer(), event.getClickedBlock(), getTable(event.getClickedBlock()))).start();
                else {
                    switch (event.getMaterial()) {
                        case BUCKET:
                            queueBlock(event.getPlayer().getName(), event.getClickedBlock(), event.getClickedBlock().getTypeId(), 0, event.getClickedBlock().getData());
                            break;
                        case WATER_BUCKET:
                            queueBlock(event.getPlayer(), event.getClickedBlock().getFace(event.getBlockFace()), Material.STATIONARY_WATER.getId());
                            break;
                        case LAVA_BUCKET:
                            queueBlock(event.getPlayer(), event.getClickedBlock().getFace(event.getBlockFace()), Material.STATIONARY_LAVA.getId());
                            break;
                        case FLINT_AND_STEEL:
                            queueBlock(event.getPlayer(), event.getClickedBlock().getFace(event.getBlockFace()), Material.FIRE.getId());
                            break;
                        case SEEDS:
                            queueBlock(event.getPlayer(), event.getClickedBlock().getFace(event.getBlockFace()), Material.CROPS.getId());
                            break;
                        case WOOD_HOE:
                        case STONE_HOE:
                        case IRON_HOE:
                        case DIAMOND_HOE:
                        case GOLD_HOE:
                            queueBlock(event.getPlayer().getName(), event.getClickedBlock(), event.getClickedBlock().getTypeId(), Material.SOIL.getId(), (byte)0);
                            break;
                    }
				}
			}
		}
		
	    public void onPlayerJoin(PlayerEvent event) {
	    	Connection conn = getConnection();
	    	Statement state = null;
			if (conn == null)
				return;
			try {
				state = conn.createStatement();
				state.execute("INSERT IGNORE INTO `lb-players` (`playername`) VALUES ('" + event.getPlayer().getName() + "');");
			} catch (SQLException ex) {
				log.log(Level.SEVERE, "[LogBlock] SQL exception", ex);
			} finally {
				try {
					if (state != null)
						state.close();
					if (conn != null)
						conn.close();
				} catch (SQLException ex) {
					log.log(Level.SEVERE, "[LogBlock] SQL exception on close", ex);
				}
			}
	    }
	}

	private class LBBlockListener extends BlockListener
	{
		
		public void onBlockPlace(BlockPlaceEvent event) {
			if (!event.isCancelled()) {
				if (event.getItemInHand().getTypeId() == getAssignedBlock(event.getPlayer()) &&
                        (CheckPermission(event.getPlayer(), "logblock.lookup") || CheckPermissionWorldGuard(event.getPlayer(), event.getBlock().getLocation()))) {
					new Thread(new BlockStats(getConnection(), event.getPlayer(), event.getBlock(), getTable(event.getBlock()))).start();
					if (config.toolblockRemove)
						event.setCancelled(true);
				} else
					queueBlock(event.getPlayer().getName(), event.getBlockPlaced(), event.getBlockReplacedState().getTypeId(), event.getBlockPlaced().getTypeId(), event.getBlockPlaced().getData());
			}
		}
	
		public void onBlockBreak(BlockBreakEvent event) {
			if (!event.isCancelled())
				queueBlock(event.getPlayer().getName(), event.getBlock(), event.getBlock().getTypeId(), 0, event.getBlock().getData());
		}
	
		public void onSignChange(SignChangeEvent event) {
			if (!event.isCancelled())
				if (config.logSignTexts)
					queueBlock(event.getPlayer().getName(), event.getBlock(), 0, event.getBlock().getTypeId(), event.getBlock().getData(), "sign [" + event.getLine(0) + "] [" + event.getLine(1) + "] [" + event.getLine(2) + "] [" + event.getLine(3) + "]", null);
				else
					queueBlock(event.getPlayer().getName(), event.getBlock(), 0, event.getBlock().getTypeId(), event.getBlock().getData());
		}
	
		public void onBlockBurn(BlockBurnEvent event) {
			if (!event.isCancelled())
				queueBlock(config.logFireAs, event.getBlock(), event.getBlock().getTypeId(), 0, event.getBlock().getData());
		}
	    
	    public void onLeavesDecay(LeavesDecayEvent event) {
	    	if (!event.isCancelled())
	    		queueBlock(config.logLeavesDecayAs, event.getBlock(), event.getBlock().getTypeId(), 0, event.getBlock().getData());
	    }
	}

	private class LBEntityListener extends EntityListener
	{
		public void onEntityExplode(EntityExplodeEvent event) {
		if (!event.isCancelled()) {	
			String name;
			if (event.getEntity() == null) 
				name = config.logTNTExplosionsAs;
			else
				name = config.logCreeperExplosionsAs;
			for (Block block : event.blockList())
				queueBlock(name, block, block.getTypeId(), 0, block.getData());
			}
		}
	}

	private boolean isInt(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException ex) {
			return false;
		}
	}

	private class Consumer implements Runnable
	{
		private boolean stop = false;

		Consumer() {
			stop = false;
		}

		public void stop() {
			stop = true;
		}

		public void run() {
			PreparedStatement ps = null;
			Connection conn = null;
			BlockRow b;
			while (!stop) {
				long start = System.currentTimeMillis()/1000L;
				int count = 0;
				if (bqueue.size() > 100)
					log.info("[LogBlock] Queue overloaded. Size: " + bqueue.size());				
				try {
					conn = getConnection();
					conn.setAutoCommit(false);
					while (count < 100 && start+config.delay > (System.currentTimeMillis()/1000L)) {
						b = bqueue.poll(1L, TimeUnit.SECONDS);
						if (b == null)
							continue;
						ps = conn.prepareStatement("INSERT INTO `" + b.table + "` (`date`, `playerid`, `replaced`, `type`, `data`, `x`, `y`, `z`) SELECT now(), `playerid`, ?, ?, ?, ?, ? , ? FROM `lb-players` WHERE `playername` = ?", Statement.RETURN_GENERATED_KEYS);
						ps.setInt(1, b.replaced);
						ps.setInt(2, b.type);
						ps.setByte(3, b.data);
						ps.setInt(4, b.x);
						ps.setInt(5, b.y);
						ps.setInt(6, b.z);
						ps.setString(7, b.name);
						ps.executeUpdate();
						if (b.signtext != null) {
							ResultSet keys = ps.getGeneratedKeys();
							keys.next();
							int key = keys.getInt(1);
							ps = conn.prepareStatement("INSERT INTO `" + b.table + "-sign` (`id`, `signtext`) values (?,?)");
							ps.setInt(1, key);
							ps.setString(2, b.signtext);
							ps.executeUpdate();
						} else if (b.ca != null) {
							ResultSet keys = ps.getGeneratedKeys();
							keys.next();
							int key = keys.getInt(1);
							ps = conn.prepareStatement("INSERT INTO `" + b.table + "-chest` (`id`, `intype`, `inamount`, `outtype`, `outamount`) values (?,?,?,?,?)");
							ps.setInt(1, key);
							ps.setShort(2, b.ca.inType);
							ps.setByte(3, b.ca.inAmount);
							ps.setShort(4, b.ca.outType);
							ps.setByte(5, b.ca.outAmount);
							ps.executeUpdate();
						}
						count++;
					}
					conn.commit();
				} catch (InterruptedException ex) {
					log.log(Level.SEVERE, "[LogBlock] Interrupted exception", ex);
				} catch (SQLException ex) {
					log.log(Level.SEVERE, "[LogBlock] SQL exception", ex);
				} finally {
					try {
						if (ps != null)
							ps.close();
						if (conn != null)
							conn.close();
					} catch (SQLException ex) {
						log.log(Level.SEVERE, "[LogBlock] SQL exception on close", ex);
					}
				}
			}
		}
	}
	
	private class ChestAccess
	{
		public short inType, outType;
		public byte inAmount, outAmount;
		
		ChestAccess(short inType, byte inAmount, short outType, byte outAmount) {
			this.inType = inType;
			this.inAmount = inAmount;
			this.outType = outType;
			this.outAmount = outAmount;
		}
	}
	
	private class BlockRow
	{
		public String table;
		public String name;
		public int replaced, type;
		public byte data;
		public int x, y, z;
		public String signtext;
		public ChestAccess ca;

		BlockRow(String table, String name, int replaced, int type, byte data, int x, int y, int z)	{
			this.table = table;
			this.name = name;
			this.replaced = replaced;
			this.type = type;
			this.data = data;
			this.x = x;
			this.y = y;
			this.z = z;
			this.signtext = null;
			this.ca = null;
		}
	}
}
