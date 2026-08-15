import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
SYNC_GUILD_ID = os.getenv("SYNC_GUILD_ID", "").strip()

DB_PATH = os.getenv(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "discord_favorites.db"),
).strip()

PER_PAGE = 10

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN 未设置，请配置 .env")


def utc_now_iso() -> str:
    """返回带 UTC 时区的 ISO 8601 字符串，便于 SQLite 排序和比较。"""
    return datetime.now(timezone.utc).isoformat()


def parse_db_datetime(value) -> datetime:
    """把 SQLite 中保存的时间字符串转换成带时区的 datetime。"""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if not value:
        return datetime.now(timezone.utc)

    text = str(value).strip()
    try:
        result = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        # 兼容 SQLite CURRENT_TIMESTAMP 产生的旧格式
        result = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )

    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result


class SQLiteResult:
    """给数据库 execute 调用提供 rowcount。"""

    def __init__(self, rowcount: int):
        self.rowcount = rowcount


class SQLiteConnection:
    """对 aiosqlite 连接做一层很小的兼容封装。"""

    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    @staticmethod
    def _params(params):
        converted = []
        for value in params:
            if isinstance(value, datetime):
                converted.append(value.astimezone(timezone.utc).isoformat())
            elif isinstance(value, bool):
                converted.append(1 if value else 0)
            else:
                converted.append(value)
        return tuple(converted)

    async def execute(self, sql: str, *params):
        cursor = await self.conn.execute(sql, self._params(params))
        return SQLiteResult(cursor.rowcount)

    async def fetchrow(self, sql: str, *params):
        cursor = await self.conn.execute(sql, self._params(params))
        return await cursor.fetchone()

    async def fetchval(self, sql: str, *params):
        cursor = await self.conn.execute(sql, self._params(params))
        row = await cursor.fetchone()
        return row[0] if row is not None else None

    async def fetch(self, sql: str, *params):
        cursor = await self.conn.execute(sql, self._params(params))
        return await cursor.fetchall()


class _SQLiteAcquire:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        return self.pool.conn_wrapper

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self.pool.conn.commit()
        else:
            await self.pool.conn.rollback()


class SQLitePool:
    """轻量 SQLite 连接池兼容层；本项目使用单个 SQLite 连接即可。"""

    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn
        self.conn_wrapper = SQLiteConnection(conn)

    def acquire(self):
        return _SQLiteAcquire(self)

    async def close(self):
        await self.conn.close()


async def open_database() -> SQLitePool:
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)

    conn = await aiosqlite.connect(DB_PATH)
    conn.row_factory = aiosqlite.Row

    # SQLite 外键约束和 WAL 模式。
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.commit()

    log.info("SQLite 数据库已打开：%s", DB_PATH)
    return SQLitePool(conn)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("favorite-bot")




def thread_url(guild_id: int, thread_id: int) -> str:
    return f"https://discord.com/channels/{guild_id}/{thread_id}"


async def is_forum_thread(message: discord.Message) -> bool:
    """
    可靠判断消息是否位于 Discord Forum 帖子。

    不只依赖本地缓存的 message.channel.parent：
    - 优先使用当前 channel 对象；
    - 如果 parent 信息缺失，则通过 guild.fetch_channel() 重新获取 Thread；
    - 再通过 parent_id 获取真正的 Forum Channel。

    这样同一个机器人可以同时处理多个服务器中的 Forum。
    """
    channel = message.channel

    # 正常情况下，Discord 会直接把 Forum 帖子的消息所属 Thread
    # 作为 message.channel 传进来。
    if isinstance(channel, discord.Thread):
        parent = channel.parent

        # parent 已经在缓存中，并且确实是 Forum。
        if parent is not None and parent.type == discord.ChannelType.forum:
            return True

        # 缓存里没有 parent，使用 parent_id 进一步确认。
        parent_id = getattr(channel, "parent_id", None)
        if parent_id and message.guild is not None:
            try:
                parent_channel = message.guild.get_channel(parent_id)
                if parent_channel is None:
                    parent_channel = await message.guild.fetch_channel(parent_id)

                if parent_channel is not None:
                    return parent_channel.type == discord.ChannelType.forum
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        # 如果当前 Thread 的 parent 信息不完整，重新获取 Thread。
        if message.guild is not None:
            try:
                fresh_channel = await message.guild.fetch_channel(channel.id)
                if isinstance(fresh_channel, discord.Thread):
                    parent_id = getattr(fresh_channel, "parent_id", None)

                    if fresh_channel.parent is not None:
                        return (
                            fresh_channel.parent.type
                            == discord.ChannelType.forum
                        )

                    if parent_id:
                        parent_channel = message.guild.get_channel(parent_id)
                        if parent_channel is None:
                            parent_channel = await message.guild.fetch_channel(
                                parent_id
                            )

                        return (
                            parent_channel is not None
                            and parent_channel.type == discord.ChannelType.forum
                        )
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

    return False


def is_private_context(interaction: discord.Interaction) -> bool:
    return interaction.guild is not None


def help_embed() -> discord.Embed:
    embed = discord.Embed(
        title="📌 Discord 收藏机器人",
        description=(
            "用于收藏 Discord Forum 论坛帖子。\n\n"
            "**📌 收藏帖子**\n"
            "在论坛帖子中的任意消息上右键 → Apps → 📌 收藏帖子。\n\n"
            "**📕 取消收藏**\n"
            "右键帖子中的任意消息 → Apps → 📕 取消收藏。\n\n"
            "**📚 我的收藏**\n"
            "`/favorites`：服务器内查看当前服务器收藏；"
            "私信机器人时查看全部服务器收藏。\n\n"
            "**🏆 排行榜**\n"
            "`/top`：历史累计 Top 10。\n"
            "`/top30`：最近 30 天 Top 10。\n\n"
            "收藏关系不会公开给其他用户，排行榜只显示收藏数量。"
        ),
        color=discord.Color.blurple(),
    )
    embed.set_footer(text="简体中文 · English localization reserved")
    return embed


class FavoriteBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        super().__init__(
            command_prefix=commands.when_mentioned,
            intents=intents,
        )
        self.db: Optional[SQLitePool] = None

    async def setup_hook(self) -> None:
        self.db = await open_database()
        await self.init_db()

        if SYNC_GUILD_ID:
            guild = discord.Object(id=int(SYNC_GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            log.info("应用命令已同步到测试服务器 %s", SYNC_GUILD_ID)
        else:
            await self.tree.sync()
            log.info("应用命令已进行全局同步；全球同步可能需要一段时间生效")

    async def init_db(self) -> None:
        assert self.db is not None

        async with self.db.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    first_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    last_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    help_dm_sent INTEGER NOT NULL DEFAULT 0
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS favorites (
                    user_id INTEGER NOT NULL,
                    guild_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    PRIMARY KEY (user_id, thread_id)
                )
                """
            )

            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_favorites_guild_thread
                ON favorites (guild_id, thread_id)
                """
            )

            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_favorites_user_guild_created
                ON favorites (user_id, guild_id, created_at DESC)
                """
            )

            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_favorites_guild_created
                ON favorites (guild_id, created_at DESC)
                """
            )

        log.info("SQLite 数据库初始化完成：%s", DB_PATH)

    async def ensure_user(self, user: discord.abc.User) -> None:
        assert self.db is not None

        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT help_dm_sent FROM users WHERE user_id=?",
                user.id,
            )

            if row is None:
                await conn.execute(
                    "INSERT INTO users (user_id) VALUES (?)",
                    user.id,
                )
                first_use = True
            else:
                await conn.execute(
                    "UPDATE users SET last_seen_at=strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now') WHERE user_id=?",
                    user.id,
                )
                first_use = False

        if first_use:
            try:
                await user.send(embed=help_embed())

                async with self.db.acquire() as conn:
                    await conn.execute(
                        "UPDATE users SET help_dm_sent=1 WHERE user_id=?",
                        user.id,
                    )
            except (discord.Forbidden, discord.HTTPException) as exc:
                log.info("无法向首次使用用户 %s 发送 DM: %s", user.id, exc)

    async def on_ready(self) -> None:
        log.info(
            "机器人已上线：%s (%s)",
            self.user,
            self.user.id if self.user else "?",
        )
        log.info("已连接服务器：%d", len(self.guilds))

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()
        await super().close()


bot = FavoriteBot()


async def ensure_user(interaction: discord.Interaction) -> None:
    await bot.ensure_user(interaction.user)


async def favorite_message(
    interaction: discord.Interaction,
    message: discord.Message,
) -> None:
    await ensure_user(interaction)
    ephemeral = is_private_context(interaction)

    # 先 defer，避免 is_forum_thread() 的网络请求 + 数据库查询
    # 耗时超过 3 秒导致 interaction token 失效（Unknown interaction）。
    await interaction.response.defer(ephemeral=ephemeral, thinking=True)

    if not await is_forum_thread(message):
        await interaction.followup.send(
            "❌ 这个消息不属于 Discord Forum 帖子。",
            ephemeral=ephemeral,
        )
        return

    if interaction.guild is None:
        await interaction.followup.send(
            "❌ 收藏帖子必须来自服务器。",
            ephemeral=True,
        )
        return

    assert bot.db is not None
    guild_id = interaction.guild.id
    thread_id = message.channel.id

    async with bot.db.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO favorites (user_id, guild_id, thread_id)
            VALUES (?, ?, ?)
            ON CONFLICT (user_id, thread_id) DO NOTHING
            """,
            interaction.user.id,
            guild_id,
            thread_id,
        )

        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM favorites
            WHERE guild_id=? AND thread_id=?
            """,
            guild_id,
            thread_id,
        )

    url = thread_url(guild_id, thread_id)

    if result.rowcount == 1:
        title = "✅ 收藏成功"
        text = f"{url}\n\n📌 当前收藏：**{count}**"
        color = discord.Color.green()
    else:
        title = "📌 已经收藏"
        text = (
            f"你已经收藏过这个帖子。\n\n"
            f"{url}\n\n"
            f"📌 当前收藏：**{count}**"
        )
        color = discord.Color.blurple()

    await interaction.followup.send(
        embed=discord.Embed(
            title=title,
            description=text,
            color=color,
        ),
        ephemeral=ephemeral,
    )


async def unfavorite_message(
    interaction: discord.Interaction,
    message: discord.Message,
) -> None:
    await ensure_user(interaction)
    ephemeral = is_private_context(interaction)

    # 同样先 defer，理由与 favorite_message 中一致。
    await interaction.response.defer(ephemeral=ephemeral, thinking=True)

    if not await is_forum_thread(message):
        await interaction.followup.send(
            "❌ 这个消息不属于 Discord Forum 帖子。",
            ephemeral=ephemeral,
        )
        return

    if interaction.guild is None:
        await interaction.followup.send(
            "❌ 取消收藏必须来自服务器。",
            ephemeral=True,
        )
        return

    assert bot.db is not None

    async with bot.db.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM favorites
            WHERE user_id=? AND guild_id=? AND thread_id=?
            """,
            interaction.user.id,
            interaction.guild.id,
            message.channel.id,
        )

    text = (
        "📕 已取消收藏。"
        if result.rowcount == 1
        else "ℹ️ 你还没有收藏这个帖子。"
    )

    await interaction.followup.send(
        text,
        ephemeral=ephemeral,
    )


@app_commands.context_menu(name="📌 收藏帖子")
async def favorite_post(
    interaction: discord.Interaction,
    message: discord.Message,
) -> None:
    await favorite_message(interaction, message)


@app_commands.context_menu(name="📕 取消收藏")
async def unfavorite_post(
    interaction: discord.Interaction,
    message: discord.Message,
) -> None:
    await unfavorite_message(interaction, message)


bot.tree.add_command(favorite_post)
bot.tree.add_command(unfavorite_post)


class FavoritesView(discord.ui.View):
    def __init__(
        self,
        owner_id: int,
        guild_id: Optional[int],
        page: int = 0,
    ) -> None:
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.guild_id = guild_id
        self.page = page

    async def interaction_check(
        self,
        interaction: discord.Interaction,
    ) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "❌ 这个收藏列表不是你的。",
                ephemeral=True,
            )
            return False
        return True

    async def load(self):
        assert bot.db is not None
        offset = self.page * PER_PAGE

        async with bot.db.acquire() as conn:
            if self.guild_id is None:
                total = await conn.fetchval(
                    "SELECT COUNT(*) FROM favorites WHERE user_id=?",
                    self.owner_id,
                )
                rows = await conn.fetch(
                    """
                    SELECT guild_id, thread_id, created_at
                    FROM favorites
                    WHERE user_id=?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    self.owner_id,
                    PER_PAGE,
                    offset,
                )
            else:
                total = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM favorites
                    WHERE user_id=? AND guild_id=?
                    """,
                    self.owner_id,
                    self.guild_id,
                )
                rows = await conn.fetch(
                    """
                    SELECT guild_id, thread_id, created_at
                    FROM favorites
                    WHERE user_id=? AND guild_id=?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    self.owner_id,
                    self.guild_id,
                    PER_PAGE,
                    offset,
                )

        pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        return total, rows, pages

    async def refresh(self, interaction: discord.Interaction) -> None:
        total, rows, pages = await self.load()
        self.page = min(self.page, pages - 1)

        self.prev.disabled = self.page <= 0
        self.next.disabled = self.page >= pages - 1

        await interaction.response.edit_message(
            embed=favorites_embed(rows, total, self.page, pages),
            view=self,
        )

    @discord.ui.button(
        label="⬅",
        style=discord.ButtonStyle.secondary,
    )
    async def prev(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        self.page -= 1
        await self.refresh(interaction)

    @discord.ui.button(
        label="➡",
        style=discord.ButtonStyle.secondary,
    )
    async def next(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        self.page += 1
        await self.refresh(interaction)


def favorites_embed(
    rows,
    total: int,
    page: int,
    pages: int,
) -> discord.Embed:
    embed = discord.Embed(
        title="📚 我的收藏",
        description=f"共 **{total}** 个收藏",
        color=discord.Color.blurple(),
    )

    if not rows:
        embed.add_field(
            name="",
            value="📭 你还没有收藏任何帖子。",
            inline=False,
        )
    else:
        lines = []

        for index, row in enumerate(
            rows,
            start=page * PER_PAGE + 1,
        ):
            url = thread_url(
                row["guild_id"],
                row["thread_id"],
            )
            created = parse_db_datetime(row["created_at"]).astimezone(
                timezone.utc
            ).strftime("%Y-%m-%d %H:%M UTC")

            lines.append(
                f"**{index}.** {url}\n"
                f"　收藏时间：`{created}`"
            )

        # Discord 单个 embed field 的 value 上限是 1024 字符。
        # PER_PAGE 条目拼在一起可能超出该限制，因此这里按长度
        # 拆分成多个 field，而不是硬塞进同一个 field 导致 400。
        MAX_FIELD_LEN = 1024
        chunk_lines: list[str] = []
        chunk_len = 0

        def flush_chunk() -> None:
            if chunk_lines:
                embed.add_field(
                    name="",
                    value="\n\n".join(chunk_lines),
                    inline=False,
                )

        for line in lines:
            # +2 对应拼接时使用的 "\n\n"
            added_len = len(line) + (2 if chunk_lines else 0)

            if chunk_len + added_len > MAX_FIELD_LEN:
                flush_chunk()
                chunk_lines = []
                chunk_len = 0
                added_len = len(line)

            chunk_lines.append(line)
            chunk_len += added_len

        flush_chunk()

    embed.set_footer(text=f"第 {page + 1} / {pages} 页")
    return embed


@bot.tree.command(
    name="favorites",
    description="查看我的收藏",
)
async def favorites(
    interaction: discord.Interaction,
) -> None:
    await ensure_user(interaction)

    guild_id = (
        interaction.guild.id
        if interaction.guild
        else None
    )

    view = FavoritesView(
        owner_id=interaction.user.id,
        guild_id=guild_id,
    )

    total, rows, pages = await view.load()

    view.prev.disabled = True
    view.next.disabled = pages <= 1

    await interaction.response.send_message(
        embed=favorites_embed(rows, total, 0, pages),
        view=view,
        ephemeral=is_private_context(interaction),
    )


async def ranking(
    interaction: discord.Interaction,
    days: Optional[int],
) -> None:
    await ensure_user(interaction)

    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ 排行榜只能在服务器中使用。",
            ephemeral=True,
        )
        return

    assert bot.db is not None
    guild_id = interaction.guild.id

    async with bot.db.acquire() as conn:
        if days is None:
            rows = await conn.fetch(
                """
                SELECT thread_id, COUNT(*) AS favorite_count
                FROM favorites
                WHERE guild_id=?
                GROUP BY thread_id
                ORDER BY favorite_count DESC, thread_id ASC
                LIMIT 10
                """,
                guild_id,
            )
            title = "🏆 服务器历史累计收藏 Top 10"
        else:
            since = datetime.now(timezone.utc) - timedelta(days=days)

            rows = await conn.fetch(
                """
                SELECT thread_id, COUNT(*) AS favorite_count
                FROM favorites
                WHERE guild_id=?
                  AND created_at >= ?
                GROUP BY thread_id
                ORDER BY favorite_count DESC, thread_id ASC
                LIMIT 10
                """,
                guild_id,
                since,
            )
            title = "🔥 服务器最近 30 天收藏 Top 10"

    embed = discord.Embed(
        title=title,
        color=discord.Color.gold(),
    )

    if not rows:
        embed.description = "📭 当前没有收藏数据。"
    else:
        medals = ["🥇", "🥈", "🥉"]
        lines = []

        for index, row in enumerate(rows, start=1):
            prefix = (
                medals[index - 1]
                if index <= 3
                else f"**{index}.**"
            )

            url = thread_url(
                guild_id,
                row["thread_id"],
            )

            lines.append(
                f"{prefix} {url}\n"
                f"　📌 **{row['favorite_count']}** 人收藏"
            )

        embed.description = "\n\n".join(lines)

    # 排行榜是服务器公开统计，因此不使用 ephemeral。
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="top",
    description="查看服务器历史累计收藏 Top 10",
)
async def top(
    interaction: discord.Interaction,
) -> None:
    await ranking(interaction, None)


@bot.tree.command(
    name="top30",
    description="查看服务器最近 30 天收藏 Top 10",
)
async def top30(
    interaction: discord.Interaction,
) -> None:
    await ranking(interaction, 30)


@bot.tree.command(
    name="help",
    description="查看收藏机器人帮助",
)
async def help_command(
    interaction: discord.Interaction,
) -> None:
    await ensure_user(interaction)

    await interaction.response.send_message(
        embed=help_embed(),
        ephemeral=is_private_context(interaction),
    )


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
) -> None:
    log.error(
        "Application Command Error: %r",
        error,
        exc_info=(type(error), error, error.__traceback__),
    )

    try:
        if interaction.response.is_done():
            await interaction.followup.send(
                "❌ 操作失败，请稍后再试。",
                ephemeral=is_private_context(interaction),
            )
        else:
            await interaction.response.send_message(
                "❌ 操作失败，请稍后再试。",
                ephemeral=is_private_context(interaction),
            )
    except discord.HTTPException:
        pass


bot.run(TOKEN)
