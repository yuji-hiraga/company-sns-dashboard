"""
SNS管理ダッシュボード
@lumina_ai_art / @neet_myaku
"""
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import tweepy
import os
import json
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

load_dotenv(dotenv_path='.env')

# Streamlit Cloud対応: secrets.tomlがあれば環境変数として使う
try:
    for k, v in st.secrets.items():
        if not os.getenv(k):
            os.environ[k] = str(v)
except Exception:
    pass

# ── 設定永続化 ────────────────────────────────────────────
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), ".dashboard_settings.json")

def load_settings() -> dict:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_setting(key: str, value):
    settings = load_settings()
    settings[key] = value
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(settings, f, default=str)
    except:
        pass

_SAVED = load_settings()

st.set_page_config(page_title="SNS Dashboard", page_icon="📊", layout="wide")

# ── カスタムCSS ───────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #f5f6fa; }
    .block-container { padding: 1.5rem 2rem; }
    h1 { color: #2d3436; font-size: 1.4rem !important; }
    h2 { color: #2d3436; font-size: 1.1rem !important; border-bottom: 2px solid #e0e0e0; padding-bottom: 6px; }
    .stTabs [data-baseweb="tab"] { font-size: 14px; }
    .stTabs [data-baseweb="tab-list"] { background: white; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ── DB接続 ────────────────────────────────────────────────
@st.cache_resource
def get_db():
    return psycopg2.connect(os.getenv("DATABASE_URL", "dbname=company_data"))

# ── X API接続 ────────────────────────────────────────────
@st.cache_resource
def get_lumina_client():
    return tweepy.Client(
        consumer_key=os.getenv("X_API_KEY"),
        consumer_secret=os.getenv("X_API_SECRET"),
        access_token=os.getenv("X_ACCESS_TOKEN"),
        access_token_secret=os.getenv("X_ACCESS_TOKEN_SECRET"),
    )

@st.cache_resource
def get_myaku_client():
    return tweepy.Client(
        consumer_key=os.getenv("OOW_API_KEY"),
        consumer_secret=os.getenv("OOW_API_SECRET"),
        access_token=os.getenv("OOW_ACCESS_TOKEN"),
        access_token_secret=os.getenv("OOW_ACCESS_TOKEN_SECRET"),
    )

# ── データ取得関数 ────────────────────────────────────────
@st.cache_data(ttl=300)
def get_account_info(account: str):
    try:
        client = get_lumina_client() if account == "lumina" else get_myaku_client()
        me = client.get_me(user_fields=["public_metrics"])
        m = me.data.public_metrics
        return {
            "followers_count": m["followers_count"],
            "following_count": m["following_count"],
            "tweet_count": m["tweet_count"],
        }
    except:
        return None

@st.cache_data(ttl=300)
def get_daily_summary(account_id: int):
    try:
        conn = get_db()
        return pd.read_sql("""
            SELECT summary_date, followers, impressions, posts_count, likes
            FROM marketing.daily_summary
            WHERE account_id = %s ORDER BY summary_date ASC
        """, conn, params=(account_id,))
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_follower_growth(account_id: int):
    try:
        conn = get_db()
        return pd.read_sql("""
            SELECT summary_date, followers, daily_change, daily_growth_pct,
                   impressions, likes, posts_count
            FROM marketing.v_follower_growth
            WHERE account_id = %s ORDER BY summary_date ASC
        """, conn, params=(account_id,))
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_posts(account_id: int):
    try:
        conn = get_db()
        return pd.read_sql("""
            SELECT p.content, p.published_at,
                   COALESCE(pm.impressions,0) as impressions,
                   COALESCE(pm.likes,0) as likes,
                   COALESCE(pm.shares,0) as retweets
            FROM marketing.posts p
            LEFT JOIN marketing.post_metrics pm ON pm.post_id = p.id
            WHERE p.account_id = %s AND p.status = 'published'
            ORDER BY p.published_at DESC LIMIT 20
        """, conn, params=(account_id,))
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_post_performance(account_id: int):
    try:
        conn = get_db()
        return pd.read_sql("""
            SELECT post_id, content, published_at, impressions, likes,
                   retweets, clicks, engagement_rate, post_hour, post_dow
            FROM marketing.v_post_performance
            WHERE username = (SELECT username FROM marketing.accounts WHERE id = %s)
            ORDER BY published_at DESC LIMIT 50
        """, conn, params=(account_id,))
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_buzz_stock(status_filter: str = "all"):
    try:
        conn = get_db()
        where = "" if status_filter == "all" else f"WHERE b.status = '{status_filter}'"
        return pd.read_sql(f"""
            SELECT b.id, b.source_username, b.original_text,
                   COALESCE(b.likes_count, 0) AS likes_count,
                   COALESCE(b.retweets_count, 0) AS retweets_count,
                   b.category, b.genre, b.arrange_idea,
                   b.status, b.created_at
            FROM marketing.buzz_references b
            {where}
            ORDER BY b.likes_count DESC LIMIT 100
        """, conn)
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_hashtag_stats():
    try:
        conn = get_db()
        return pd.read_sql("""
            SELECT h.tag, h.category, hp.account_id, a.username,
                   hp.usage_count, hp.avg_impressions, hp.avg_likes,
                   hp.avg_engagement, hp.period_start, hp.period_end
            FROM marketing.hashtag_performance hp
            JOIN marketing.hashtags h ON h.id = hp.hashtag_id
            JOIN marketing.accounts a ON a.id = hp.account_id
            ORDER BY hp.avg_impressions DESC
        """, conn)
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_competitors():
    try:
        conn = get_db()
        return pd.read_sql("""
            SELECT ca.id, ca.username, ca.display_name, ca.genre, ca.notes,
                   cm.measured_date, cm.followers, cm.following, cm.tweet_count,
                   cm.avg_likes, cm.avg_retweets
            FROM marketing.competitor_accounts ca
            LEFT JOIN marketing.competitor_metrics cm ON cm.competitor_id = ca.id
            WHERE ca.is_active = true
            ORDER BY ca.genre, cm.measured_date DESC
        """, conn)
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_scheduled_posts(account_id: int):
    try:
        conn = get_db()
        return pd.read_sql("""
            SELECT id, content, scheduled_at, status, tags, image_url
            FROM marketing.posts
            WHERE account_id = %s AND status IN ('draft', 'scheduled')
            ORDER BY scheduled_at ASC NULLS LAST
        """, conn, params=(account_id,))
    except:
        return pd.DataFrame()

# ── チャート関数 ──────────────────────────────────────────
def make_bar_chart(df, x, y, title, color1="#4A90E2"):
    if df.empty or x not in df.columns:
        fig = go.Figure()
        fig.add_annotation(text="データ蓄積中...", showarrow=False,
            font=dict(color="#bbb", size=13), xref="paper", yref="paper", x=0.5, y=0.5)
    else:
        fig = go.Figure(go.Bar(x=df[x], y=df[y], marker_color=color1, name="実績"))
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color="#555"), x=0),
        height=180, margin=dict(l=10, r=10, t=40, b=20),
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(color="#555", size=10), showlegend=False, bargap=0.3,
    )
    fig.update_xaxes(gridcolor="#f0f0f0", linecolor="#e0e0e0", tickfont_size=9)
    fig.update_yaxes(gridcolor="#f0f0f0", linecolor="#e0e0e0", tickfont_size=9)
    return fig

def make_donut_gauge(value, goal, title, actual_label, goal_label, color="#4A90E2", key=""):
    pct = min(value / goal * 100, 100) if goal > 0 else 0
    diff = value - goal
    fig = go.Figure(go.Pie(
        values=[pct, max(100 - pct, 0)], hole=0.72,
        marker_colors=[color, "#f0f0f0"], textinfo="none", hoverinfo="skip", showlegend=False,
    ))
    fig.add_annotation(text=f"<b>{pct:.0f}%</b>", x=0.5, y=0.55,
        font=dict(size=20, color="#2d3436"), showarrow=False, xref="paper", yref="paper")
    fig.add_annotation(
        text=f"{'▲' if diff >= 0 else '▼'} {abs(diff):,}", x=0.5, y=0.35,
        font=dict(size=10, color="#e74c3c" if diff < 0 else "#27ae60"),
        showarrow=False, xref="paper", yref="paper")
    fig.update_layout(
        title=dict(text=f"<b>{title}</b>", font=dict(size=12, color="#555"), x=0.5, xanchor="center"),
        height=180, margin=dict(l=10, r=10, t=40, b=10), paper_bgcolor="white",
        annotations=fig.layout.annotations + (
            go.layout.Annotation(
                text=f"<span style='color:#888;font-size:9px'>目標 {goal_label}<br>実績 {actual_label}</span>",
                x=0.5, y=-0.08, showarrow=False, xref="paper", yref="paper",
                font=dict(size=9, color="#aaa")),))
    st.plotly_chart(fig, use_container_width=True, key=key)

# ── SocialDog風 マルチメトリックカード ──────────────────
def make_metric_card(metrics: list, df: pd.DataFrame, x_col: str, prefix: str, key: str):
    """
    SocialDog風カード: 複数メトリック + 折れ線グラフ
    metrics: [{label, value, delta, color, y_col}, ...]
    """
    cols = st.columns(len(metrics))
    for i, m in enumerate(metrics):
        with cols[i]:
            delta = m.get("delta", 0)
            if delta > 0:
                arrow, color = "↑", "#27ae60"
            elif delta < 0:
                arrow, color = "↓", "#e74c3c"
            else:
                arrow, color = "→", "#888"
            st.markdown(f"""
            <div style="padding:8px 4px;">
                <div style="display:flex; align-items:center; gap:6px;">
                    <span style="width:8px;height:8px;border-radius:50%;background:{m['color']};display:inline-block;"></span>
                    <span style="font-size:13px;color:#555;font-weight:500;">{m['label']}</span>
                </div>
                <div style="font-size:32px;font-weight:bold;color:#2d3436;line-height:1.2;margin-top:4px;">{m['value']}</div>
                <div style="font-size:12px;color:{color};margin-top:2px;">{arrow}{abs(delta) if delta != 0 else 0}</div>
            </div>
            """, unsafe_allow_html=True)

    fig = go.Figure()
    if not df.empty and x_col in df.columns:
        for m in metrics:
            y_col = m.get("y_col")
            if y_col and y_col in df.columns:
                fig.add_trace(go.Scatter(
                    x=df[x_col], y=df[y_col],
                    mode="lines+markers", name=m["label"],
                    line=dict(color=m["color"], width=2),
                    marker=dict(size=5),
                    showlegend=False,
                ))
    else:
        fig.add_annotation(text="データ蓄積中...", showarrow=False,
            font=dict(color="#bbb", size=12), xref="paper", yref="paper", x=0.5, y=0.5)

    fig.update_layout(
        height=200, margin=dict(l=10, r=10, t=10, b=20),
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(color="#888", size=10),
        xaxis=dict(gridcolor="#f5f5f5", showline=False, tickfont=dict(size=9)),
        yaxis=dict(gridcolor="#f5f5f5", showline=False, tickfont=dict(size=9)),
    )
    st.plotly_chart(fig, use_container_width=True, key=key)

def make_growth_chart(df, prefix):
    if df.empty or "summary_date" not in df.columns:
        st.info("フォロワー増減データ蓄積中...")
        return
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["summary_date"], y=df["followers"],
        mode="lines+markers", name="フォロワー数",
        line=dict(color="#4A90E2", width=2), marker=dict(size=6),
    ))
    if "daily_change" in df.columns:
        df_change = df.dropna(subset=["daily_change"])
        if not df_change.empty:
            colors = ["#27ae60" if v >= 0 else "#e74c3c" for v in df_change["daily_change"]]
            fig.add_trace(go.Bar(
                x=df_change["summary_date"], y=df_change["daily_change"],
                name="日次増減", marker_color=colors, yaxis="y2", opacity=0.5,
            ))
    fig.update_layout(
        title=dict(text="フォロワー推移 & 日次増減", font=dict(size=13, color="#555")),
        height=250, margin=dict(l=10, r=10, t=40, b=20),
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(color="#555", size=10), legend=dict(orientation="h", y=1.12),
        yaxis=dict(title="フォロワー数", gridcolor="#f0f0f0"),
        yaxis2=dict(title="日次増減", overlaying="y", side="right", gridcolor="#f0f0f0"),
    )
    st.plotly_chart(fig, use_container_width=True, key=f"{prefix}_growth")

# ── アカウントタブ ────────────────────────────────────────
def render_tab(account: str, account_id: int, goals: dict, prefix: str):
    info = get_account_info(account)
    summary = get_daily_summary(account_id)
    growth = get_follower_growth(account_id)
    posts = get_posts(account_id)
    perf = get_post_performance(account_id)
    scheduled = get_scheduled_posts(account_id)

    followers = tweet_count = following = 0
    if info:
        followers = info.get("followers_count", 0)
        tweet_count = info.get("tweet_count", 0)
        following = info.get("following_count", 0)

    total_impressions = int(summary["impressions"].sum()) if not summary.empty else 0
    total_likes = int(summary["likes"].sum()) if not summary.empty else 0

    # ── ヘッダー: 期間表示 + 期間切替 ─────────────────
    header_col1, header_col2, header_col3 = st.columns([2, 2, 1])
    with header_col1:
        st.markdown(f"**ダッシュボード**")
    with header_col2:
        if not summary.empty:
            date_from_disp = summary["summary_date"].min()
            date_to_disp = summary["summary_date"].max()
            st.caption(f"📅 {date_from_disp} - {date_to_disp}")
    with header_col3:
        st.radio("期間", ["日", "週", "月"], horizontal=True, label_visibility="collapsed", key=f"{prefix}_period_view")

    # 変化量計算（最新 vs 前日）
    def calc_delta(col):
        if summary.empty or col not in summary.columns or len(summary) < 2:
            return 0
        return int(summary[col].iloc[-1] - summary[col].iloc[-2])

    # 累計
    total_posts = int(summary["posts_count"].sum()) if not summary.empty else 0
    total_followers = int(summary["followers"].iloc[-1]) if not summary.empty else followers
    total_follows = int(summary["follows"].iloc[-1]) if not summary.empty and "follows" in summary.columns else following
    ff_ratio = round(total_followers / total_follows, 2) if total_follows > 0 else 0

    # ── 上段: 投稿数・いいね・インプレ（個別グラフ3つ）─
    eng_col1, eng_col2, eng_col3 = st.columns(3)
    with eng_col1:
        st.markdown('<div style="background:white;border-radius:12px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);margin-bottom:12px;">', unsafe_allow_html=True)
        make_metric_card(
            metrics=[{"label": "投稿数", "value": f"{total_posts:,}", "delta": calc_delta("posts_count"), "color": "#3498db", "y_col": "posts_count"}],
            df=summary, x_col="summary_date", prefix=prefix, key=f"{prefix}_card_posts"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    with eng_col2:
        st.markdown('<div style="background:white;border-radius:12px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);margin-bottom:12px;">', unsafe_allow_html=True)
        make_metric_card(
            metrics=[{"label": "いいね", "value": f"{total_likes:,}", "delta": calc_delta("likes"), "color": "#e74c3c", "y_col": "likes"}],
            df=summary, x_col="summary_date", prefix=prefix, key=f"{prefix}_card_likes"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    with eng_col3:
        st.markdown('<div style="background:white;border-radius:12px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);margin-bottom:12px;">', unsafe_allow_html=True)
        make_metric_card(
            metrics=[{"label": "インプレ", "value": f"{total_impressions:,}", "delta": calc_delta("impressions"), "color": "#27ae60", "y_col": "impressions"}],
            df=summary, x_col="summary_date", prefix=prefix, key=f"{prefix}_card_imp"
        )
        st.markdown('</div>', unsafe_allow_html=True)

    # ── 下段: フォロワー・フォロー・FF比（個別グラフ3つ）─
    fol_col1, fol_col2, fol_col3 = st.columns(3)
    with fol_col1:
        st.markdown('<div style="background:white;border-radius:12px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);margin-bottom:12px;">', unsafe_allow_html=True)
        make_metric_card(
            metrics=[{"label": "フォロワー", "value": f"{total_followers:,}", "delta": calc_delta("followers"), "color": "#1da1f2", "y_col": "followers"}],
            df=summary, x_col="summary_date", prefix=prefix, key=f"{prefix}_card_followers"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    with fol_col2:
        st.markdown('<div style="background:white;border-radius:12px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);margin-bottom:12px;">', unsafe_allow_html=True)
        delta_follows = calc_delta("follows") if "follows" in summary.columns else 0
        make_metric_card(
            metrics=[{"label": "フォロー", "value": f"{total_follows:,}", "delta": delta_follows, "color": "#888888", "y_col": "follows"}],
            df=summary, x_col="summary_date", prefix=prefix, key=f"{prefix}_card_follows"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    with fol_col3:
        st.markdown('<div style="background:white;border-radius:12px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);margin-bottom:12px;">', unsafe_allow_html=True)
        # FF比は折れ線がないのでカード単独表示
        if not summary.empty and "follows" in summary.columns:
            ff_series = (summary["followers"] / summary["follows"].replace(0, 1)).round(2)
            ff_df = pd.DataFrame({"summary_date": summary["summary_date"], "ff_ratio": ff_series})
            make_metric_card(
                metrics=[{"label": "FF比", "value": f"{ff_ratio}", "delta": 0, "color": "#9b59b6", "y_col": "ff_ratio"}],
                df=ff_df, x_col="summary_date", prefix=prefix, key=f"{prefix}_card_ff"
            )
        else:
            make_metric_card(
                metrics=[{"label": "FF比", "value": f"{ff_ratio}", "delta": 0, "color": "#9b59b6", "y_col": None}],
                df=pd.DataFrame(), x_col="summary_date", prefix=prefix, key=f"{prefix}_card_ff"
            )
        st.markdown('</div>', unsafe_allow_html=True)

    # ── フォロワー増減グラフ ─────────────────────────────
    st.markdown('<div style="background:white;border-radius:12px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,0.06);margin-bottom:12px;">', unsafe_allow_html=True)
    make_growth_chart(growth, prefix)
    st.markdown('</div>', unsafe_allow_html=True)

    # ── KPIゲージ 4x2 ───────────────────────────────────
    st.markdown("### 📌 投稿KPI")
    row1 = st.columns(4)
    with row1[0]:
        make_donut_gauge(tweet_count, goals["tweets"], "ツイート", f"{tweet_count:,}", f"{goals['tweets']:,}", "#4A90E2", f"{prefix}_g1")
    with row1[1]:
        make_donut_gauge(total_impressions, goals["impressions"], "インプレッション", f"{total_impressions:,}", f"{goals['impressions']:,}", "#FF6B35", f"{prefix}_g2")
    with row1[2]:
        make_donut_gauge(total_likes + tweet_count, goals["engagement"], "エンゲージメント", f"{total_likes+tweet_count:,}", f"{goals['engagement']:,}", "#27ae60", f"{prefix}_g3")
    with row1[3]:
        make_donut_gauge(followers, goals["followers"], "新規フォロワー数", f"{followers:,}", f"{goals['followers']:,}", "#e74c3c", f"{prefix}_g4")

    row2 = st.columns(4)
    with row2[0]:
        make_donut_gauge(total_likes, goals["likes"], "いいね", f"{total_likes:,}", f"{goals['likes']:,}", "#f39c12", f"{prefix}_g5")
    with row2[1]:
        make_donut_gauge(0, goals["retweets"], "リツイート", "0", f"{goals['retweets']:,}", "#3498db", f"{prefix}_g6")
    with row2[2]:
        make_donut_gauge(0, goals["clicks"], "リンククリック", "0", f"{goals['clicks']:,}", "#1abc9c", f"{prefix}_g7")
    with row2[3]:
        make_donut_gauge(following, goals["following"], "プロフィールクリック", f"{following:,}", f"{goals['following']:,}", "#9b59b6", f"{prefix}_g8")

    st.divider()

    # ── 投稿パフォーマンス分析 ────────────────────────────
    st.markdown("### 📊 投稿パフォーマンス")
    if not perf.empty:
        # 時間帯別エンゲージメント率
        if "post_hour" in perf.columns and perf["engagement_rate"].sum() > 0:
            hour_perf = perf.groupby("post_hour").agg(
                avg_engagement=("engagement_rate", "mean"),
                count=("post_id", "count")
            ).reset_index()
            fig_hour = go.Figure(go.Bar(
                x=hour_perf["post_hour"], y=hour_perf["avg_engagement"],
                marker_color="#FF6B35", text=hour_perf["count"],
                textposition="outside", texttemplate="%{text}件",
            ))
            fig_hour.update_layout(
                title="時間帯別エンゲージメント率",
                height=200, margin=dict(l=10, r=10, t=40, b=20),
                paper_bgcolor="white", plot_bgcolor="white",
                xaxis_title="投稿時間", yaxis_title="エンゲージメント率(%)",
            )
            st.plotly_chart(fig_hour, use_container_width=True, key=f"{prefix}_hour_perf")

        st.dataframe(
            perf[["published_at", "content", "impressions", "likes", "retweets", "engagement_rate"]].rename(columns={
                "published_at": "投稿日時", "content": "内容",
                "impressions": "インプレ", "likes": "いいね",
                "retweets": "RT", "engagement_rate": "ER%"
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("投稿データ蓄積中...")

    st.divider()

    # ── 予約投稿キュー ────────────────────────────────────
    st.markdown("### 📅 予約投稿キュー")
    if not scheduled.empty:
        st.dataframe(
            scheduled.rename(columns={
                "content": "内容", "scheduled_at": "予定日時",
                "status": "ステータス", "tags": "タグ"
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("予約投稿なし — postsテーブルにstatus='scheduled'で登録すると表示されます")

# ── バズストックタブ ──────────────────────────────────────
def render_buzz_tab():
    st.markdown("### 🔥 バズ投稿ストック")

    col_filter, col_add = st.columns([1, 1])
    with col_filter:
        status_filter = st.selectbox("ステータス", ["all", "stocked", "planned", "used", "rejected"], key="buzz_filter")
    with col_add:
        with st.expander("➕ ネタを追加"):
            new_text = st.text_area("元ツイート内容", key="buzz_new_text")
            new_col1, new_col2 = st.columns(2)
            with new_col1:
                new_source = st.text_input("元アカウント", placeholder="@username", key="buzz_source")
                new_likes = st.number_input("いいね数", min_value=0, value=0, key="buzz_likes")
            with new_col2:
                new_genre = st.selectbox("ジャンル", ["あるある", "時事", "エロ", "名言", "自虐", "その他"], key="buzz_genre")
                new_category = st.selectbox("カテゴリ", ["trend", "evergreen", "seasonal"], key="buzz_cat")
            new_arrange = st.text_area("ミャクやん風アレンジ案", key="buzz_arrange")

            if st.button("💾 保存", type="primary", key="buzz_save"):
                if new_text:
                    try:
                        conn = get_db()
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO marketing.buzz_references
                                (source_username, original_text, likes_count, genre, category, arrange_idea)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (new_source, new_text, new_likes, new_genre, new_category, new_arrange))
                        conn.commit()
                        cur.close()
                        st.success("保存しました！")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"保存エラー: {e}")

    buzz = get_buzz_stock(status_filter)
    if not buzz.empty:
        st.markdown("#### 📋 ストック一覧（直接編集できます）")
        edit_df = buzz[["id", "source_username", "original_text", "likes_count",
                        "retweets_count", "genre", "category", "arrange_idea", "status"]].copy() \
            if "retweets_count" in buzz.columns else \
            buzz[["id", "source_username", "original_text", "likes_count",
                  "genre", "category", "arrange_idea", "status"]].copy()
        # 文字列に変換（NumberColumnの入力制限回避のため）
        edit_df["likes_count"] = edit_df["likes_count"].fillna(0).astype("int64").astype(str)
        if "retweets_count" in edit_df.columns:
            edit_df["retweets_count"] = edit_df["retweets_count"].fillna(0).astype("int64").astype(str)

        edited = st.data_editor(
            edit_df,
            use_container_width=True, hide_index=True, num_rows="fixed",
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "source_username": st.column_config.TextColumn("元アカウント"),
                "original_text": st.column_config.TextColumn("元ツイート", width="large"),
                "likes_count": st.column_config.TextColumn("いいね", width="medium", help="数値で入力（例: 12000）"),
                "retweets_count": st.column_config.TextColumn("RT", width="medium", help="数値で入力（例: 5000）") if "retweets_count" in edit_df.columns else None,
                "genre": st.column_config.SelectboxColumn("ジャンル",
                    options=["あるある", "時事", "エロ", "名言", "自虐", "その他"]),
                "category": st.column_config.SelectboxColumn("カテゴリ",
                    options=["trend", "evergreen", "seasonal"]),
                "arrange_idea": st.column_config.TextColumn("アレンジ案", width="large"),
                "status": st.column_config.SelectboxColumn("状態",
                    options=["stocked", "planned", "used", "rejected"]),
            },
            key=f"buzz_editor_{status_filter}",
        )

        save_col, del_col, info_col = st.columns([1, 1, 2])
        with save_col:
            if st.button("💾 変更を保存", type="primary", key="buzz_update_btn"):
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    update_count = 0
                    original_dict = edit_df.set_index("id").to_dict("index")
                    for _, row in edited.iterrows():
                        rid = int(row["id"])
                        orig = original_dict.get(rid, {})
                        if any(row[k] != orig.get(k) for k in row.index if k != "id"):
                            def to_int(val):
                                try:
                                    return int(str(val or 0).replace(",", "").replace(" ", ""))
                                except (ValueError, TypeError):
                                    return 0
                            cur.execute("""
                                UPDATE marketing.buzz_references SET
                                    source_username = %s, original_text = %s,
                                    likes_count = %s, retweets_count = %s,
                                    genre = %s, category = %s,
                                    arrange_idea = %s, status = %s,
                                    updated_at = now()
                                WHERE id = %s
                            """, (
                                row.get("source_username"), row.get("original_text"),
                                to_int(row.get("likes_count")),
                                to_int(row.get("retweets_count")) if "retweets_count" in row else 0,
                                row.get("genre"), row.get("category"),
                                row.get("arrange_idea"), row.get("status"),
                                rid,
                            ))
                            update_count += 1
                    conn.commit()
                    cur.close()
                    if update_count > 0:
                        st.success(f"✅ {update_count}件 更新しました")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.info("変更はありません")
                except Exception as e:
                    st.error(f"更新エラー: {e}")

        with del_col:
            del_id = st.number_input("削除するID", min_value=0, value=0, step=1, key="buzz_del_id")
            if st.button("🗑️ 削除", key="buzz_del_btn"):
                if del_id > 0:
                    try:
                        conn = get_db()
                        cur = conn.cursor()
                        cur.execute("DELETE FROM marketing.buzz_references WHERE id = %s", (del_id,))
                        conn.commit()
                        cur.close()
                        st.success(f"ID {del_id} を削除しました")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"削除エラー: {e}")

        with info_col:
            st.caption(f"合計: {len(buzz)}件")
            st.caption("💡 表のセルをクリックして直接編集 → 「変更を保存」で反映")
    else:
        st.info("バズネタをストックしましょう！上の「ネタを追加」から登録できます")

    st.divider()
    st.markdown("### 🔍 バズ投稿検索（X高度な検索）")
    st.caption("条件を設定して「Xで検索」を押すと、X検索ページが開きます")

    col1, col2 = st.columns([1, 1])
    with col1:
        keyword = st.text_input("キーワード", placeholder="例: 無職, AI美女, 関西", key="buzz_search_kw")
        period_options = {
            "24時間": 1, "1週間": 7, "1ヶ月": 30, "3ヶ月": 90,
            "6ヶ月": 180, "1年": 365, "カスタム": 0,
        }
        period_keys = list(period_options.keys())
        default_period = _SAVED.get("buzz_period", "1週間")
        period_idx = period_keys.index(default_period) if default_period in period_keys else 1
        period = st.radio("期間", period_keys, index=period_idx, horizontal=True, key="buzz_period")
        save_setting("buzz_period", period)
        if period == "カスタム":
            custom_col1, custom_col2 = st.columns(2)
            twitter_birth = date(2006, 3, 21)
            saved_from = _SAVED.get("buzz_custom_from")
            saved_to = _SAVED.get("buzz_custom_to")
            default_from = date.fromisoformat(saved_from) if saved_from else date.today() - timedelta(days=7)
            default_to = date.fromisoformat(saved_to) if saved_to else date.today()
            with custom_col1:
                date_from = st.date_input("開始日",
                    value=default_from,
                    min_value=twitter_birth, max_value=date.today(), key="buzz_date_from")
            with custom_col2:
                date_to = st.date_input("終了日",
                    value=default_to,
                    min_value=twitter_birth, max_value=date.today(), key="buzz_date_to")
            save_setting("buzz_custom_from", date_from.isoformat())
            save_setting("buzz_custom_to", date_to.isoformat())
            if date_from > date_to:
                st.error("⚠️ 終了日は開始日より後の日付を指定してください")
                date_from, date_to = date_to, date_from
        else:
            days = period_options[period]
            date_from = date.today() - timedelta(days=days)
            date_to = date.today()

    engagement_scale = [0, 100, 500, 1000, 3000, 5000, 10000, 30000, 50000, 100000, 200000, 300000]
    with col2:
        default_likes = _SAVED.get("buzz_min_likes", 1000)
        default_rts = _SAVED.get("buzz_min_rts", 0)
        if default_likes not in engagement_scale: default_likes = 1000
        if default_rts not in engagement_scale: default_rts = 0
        min_likes = st.select_slider("最低いいね数", options=engagement_scale, value=default_likes, key="buzz_min_likes")
        min_rts = st.select_slider("最低RT数", options=engagement_scale, value=default_rts, key="buzz_min_rts")
        save_setting("buzz_min_likes", min_likes)
        save_setting("buzz_min_rts", min_rts)
        lang_col, sort_col = st.columns(2)
        lang_options = ["日本語のみ", "全言語", "英語のみ"]
        sort_options = ["人気順", "新着順"]
        with lang_col:
            default_lang = _SAVED.get("buzz_lang", "日本語のみ")
            lang_idx = lang_options.index(default_lang) if default_lang in lang_options else 0
            lang_option = st.selectbox("言語", lang_options, index=lang_idx, key="buzz_lang")
            save_setting("buzz_lang", lang_option)
        with sort_col:
            default_sort = _SAVED.get("buzz_sort", "人気順")
            sort_idx = sort_options.index(default_sort) if default_sort in sort_options else 0
            sort_option = st.selectbox("並び順", sort_options, index=sort_idx, key="buzz_sort")
            save_setting("buzz_sort", sort_option)

    with st.expander("🔧 追加フィルタ"):
        adv_col1, adv_col2, adv_col3 = st.columns(3)
        with adv_col1:
            exclude_words = st.text_input("除外ワード", placeholder="PR, 広告, ad", key="buzz_exclude")
        with adv_col2:
            from_account = st.text_input("特定アカウントから", placeholder="@なしで入力", key="buzz_from")
        with adv_col3:
            media_options = ["画像あり", "画像なし", "動画あり", "動画なし", "リンクあり", "リンクなし"]
            default_media = _SAVED.get("buzz_media", [])
            default_media = [m for m in default_media if m in media_options]
            media_filter = st.multiselect("メディア（複数選択可）", media_options,
                default=default_media, key="buzz_media")
            save_setting("buzz_media", media_filter)
        min_replies = st.select_slider("最低リプライ数", options=[0, 10, 50, 100, 500, 1000], value=0, key="buzz_min_replies")

    import urllib.parse

    if st.button("🔍 Xで検索", type="primary", use_container_width=True, key="buzz_search_btn"):
        query_parts = []
        if keyword:
            query_parts.append(keyword)
        if min_likes > 0:
            query_parts.append(f"min_faves:{min_likes}")
        if min_rts > 0:
            query_parts.append(f"min_retweets:{min_rts}")
        if min_replies > 0:
            query_parts.append(f"min_replies:{min_replies}")
        lang_map = {"日本語のみ": "ja", "英語のみ": "en", "全言語": None}
        lang_code = lang_map.get(lang_option)
        if lang_code:
            query_parts.append(f"lang:{lang_code}")
        if exclude_words:
            for w in exclude_words.split(","):
                w = w.strip()
                if w:
                    query_parts.append(f"-{w}")
        if from_account:
            query_parts.append(f"from:{from_account.strip('@')}")
        media_map = {
            "画像あり": "filter:images", "画像なし": "-filter:images",
            "動画あり": "filter:videos", "動画なし": "-filter:videos",
            "リンクあり": "filter:links", "リンクなし": "-filter:links",
        }
        for m in media_filter:
            if m in media_map:
                query_parts.append(media_map[m])

        if not query_parts:
            st.warning("キーワードまたはフィルタ条件を1つ以上設定してください")
        else:
            query = " ".join(query_parts)
            since_str = date_from.strftime("%Y-%m-%d")
            until_str = (date_to + timedelta(days=1)).strftime("%Y-%m-%d")
            full_query = f"{query} since:{since_str} until:{until_str}"
            sort_param = "live" if sort_option == "新着順" else "top"
            search_url = f"https://x.com/search?q={urllib.parse.quote(full_query)}&src=typed_query&f={sort_param}"

            st.markdown(f"""
            <div style="background:white; border-radius:10px; padding:16px; box-shadow:0 2px 8px rgba(0,0,0,0.08); margin:8px 0;">
                <p style="font-size:12px; color:#888; margin-bottom:4px;">生成された検索クエリ:</p>
                <code style="font-size:13px; color:#2d3436;">{full_query}</code>
                <br><br>
                <a href="{search_url}" target="_blank" style="
                    display:inline-block; background:#1da1f2; color:white;
                    padding:10px 24px; border-radius:20px; text-decoration:none;
                    font-weight:bold; font-size:14px;">
                    🔗 Xで検索結果を開く
                </a>
            </div>
            """, unsafe_allow_html=True)
            st.caption("💡 良いネタを見つけたら、上の「ネタを追加」からストックしましょう")

    st.divider()
    st.markdown("#### ⚡ クイック検索テンプレート")
    tmpl_cols = st.columns(4)
    templates = [
        {"label": "🔥 万バズ（日本語）", "kw": "", "likes": 10000, "lang": "ja"},
        {"label": "😂 おもしろ系", "kw": "おもしろ OR 草 OR ワロタ", "likes": 5000, "lang": "ja"},
        {"label": "💤 ニート・無職", "kw": "ニート OR 無職 OR 働きたくない", "likes": 3000, "lang": "ja"},
        {"label": "🎨 AI美女", "kw": "AI美女 OR AIアート OR AI art", "likes": 1000, "lang": "ja"},
    ]
    for i, tmpl in enumerate(templates):
        with tmpl_cols[i]:
            today = date.today()
            week_ago = today - timedelta(days=7)
            q = f"{tmpl['kw']} min_faves:{tmpl['likes']} lang:{tmpl['lang']} since:{week_ago} until:{today + timedelta(days=1)}" if tmpl['kw'] else f"min_faves:{tmpl['likes']} lang:{tmpl['lang']} since:{week_ago} until:{today + timedelta(days=1)}"
            url = f"https://x.com/search?q={urllib.parse.quote(q)}&src=typed_query&f=top"
            st.markdown(f"""<a href="{url}" target="_blank" style="
                display:block; background:white; border:1px solid #e0e0e0; border-radius:8px;
                padding:12px; text-align:center; text-decoration:none; color:#2d3436;
                font-size:13px; font-weight:500;">
                {tmpl['label']}
            </a>""", unsafe_allow_html=True)

# ── ハッシュタグ分析タブ ──────────────────────────────────
def render_hashtag_tab():
    st.markdown("### #️⃣ ハッシュタグ分析")

    hashtag_data = get_hashtag_stats()
    if not hashtag_data.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### インプレッション TOP ハッシュタグ")
            top_tags = hashtag_data.nlargest(15, "avg_impressions")
            fig = go.Figure(go.Bar(
                y=top_tags["tag"], x=top_tags["avg_impressions"],
                orientation="h", marker_color="#FF6B35",
                text=top_tags["avg_impressions"].apply(lambda x: f"{x:,}"),
                textposition="outside",
            ))
            fig.update_layout(
                height=400, margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="white", plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig, use_container_width=True, key="ht_top_imp")

        with col2:
            st.markdown("#### エンゲージメント率 TOP ハッシュタグ")
            top_er = hashtag_data.nlargest(15, "avg_engagement")
            fig2 = go.Figure(go.Bar(
                y=top_er["tag"], x=top_er["avg_engagement"],
                orientation="h", marker_color="#27ae60",
                text=top_er["avg_engagement"].apply(lambda x: f"{x:.1f}%"),
                textposition="outside",
            ))
            fig2.update_layout(
                height=400, margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="white", plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig2, use_container_width=True, key="ht_top_er")

        st.divider()
        st.markdown("#### 全ハッシュタグ一覧")
        st.dataframe(
            hashtag_data[["tag", "username", "usage_count", "avg_impressions", "avg_likes", "avg_engagement"]].rename(columns={
                "tag": "タグ", "username": "アカウント", "usage_count": "使用回数",
                "avg_impressions": "平均インプレ", "avg_likes": "平均いいね",
                "avg_engagement": "平均ER%",
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("ハッシュタグデータ蓄積中... 投稿にタグを付けてデータを収集すると分析結果が表示されます")

    st.divider()
    with st.expander("➕ ハッシュタグを登録"):
        new_tag = st.text_input("タグ名（#なし）", key="ht_new_tag")
        ht_col1, ht_col2 = st.columns(2)
        with ht_col1:
            new_lang = st.selectbox("言語", ["ja", "en"], key="ht_lang")
        with ht_col2:
            new_cat = st.text_input("カテゴリ", placeholder="AI美女, グラビア等", key="ht_cat")
        if st.button("💾 登録", key="ht_save"):
            if new_tag:
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO marketing.hashtags (tag, language, category)
                        VALUES (%s, %s, %s) ON CONFLICT (tag) DO NOTHING
                    """, (new_tag, new_lang, new_cat or None))
                    conn.commit()
                    cur.close()
                    st.success(f"#{new_tag} を登録しました")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"エラー: {e}")

# ── 定期投稿テンプレタブ ──────────────────────────────────
@st.cache_data(ttl=60)
def get_recurring_templates():
    try:
        conn = get_db()
        return pd.read_sql("""
            SELECT id, original_text AS title, arrange_idea AS content,
                   genre, category, status, created_at
            FROM marketing.buzz_references
            WHERE original_text LIKE '【定期投稿用テンプレ】%%'
            ORDER BY genre, id
        """, conn)
    except:
        return pd.DataFrame()

def render_recurring_tab():
    st.markdown("### 📅 定期投稿テンプレ管理")
    st.caption("SocialDog等の自動投稿ツールに登録するテンプレ集。ジャンル別に整理されています")

    templates = get_recurring_templates()
    if templates.empty:
        st.info("テンプレ未登録です")
        return

    # ジャンル統計
    genre_counts = templates["genre"].value_counts()
    cols = st.columns(len(genre_counts))
    for i, (g, c) in enumerate(genre_counts.items()):
        with cols[i]:
            st.metric(g, f"{c}件")

    st.divider()

    # ジャンル別フィルタ
    genre_filter = st.selectbox("ジャンルで絞り込む",
        ["すべて"] + sorted(templates["genre"].unique().tolist()),
        key="recur_genre_filter")

    filtered = templates if genre_filter == "すべて" else templates[templates["genre"] == genre_filter]

    # ジャンル別表示
    for genre in filtered["genre"].unique():
        st.markdown(f"#### 🏷️ {genre}")
        genre_templates = filtered[filtered["genre"] == genre]
        for _, row in genre_templates.iterrows():
            title_short = row["title"].replace("【定期投稿用テンプレ】", "").strip()
            with st.expander(f"📝 ID {row['id']}: {title_short}"):
                st.code(row["content"], language=None)
                btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 3])
                with btn_col1:
                    if st.button("📋 コピー用表示", key=f"recur_copy_{row['id']}"):
                        st.text_area("ここから全選択してコピー", row["content"], height=200, key=f"recur_text_{row['id']}")
                with btn_col2:
                    if st.button("🗑️ 削除", key=f"recur_del_{row['id']}"):
                        try:
                            conn = get_db()
                            cur = conn.cursor()
                            cur.execute("DELETE FROM marketing.buzz_references WHERE id = %s", (int(row["id"]),))
                            conn.commit()
                            cur.close()
                            st.cache_data.clear()
                            st.success(f"ID {row['id']} 削除しました")
                            st.rerun()
                        except Exception as e:
                            st.error(f"削除エラー: {e}")

    st.divider()
    st.markdown("### ➕ 新しい定期投稿テンプレを追加")
    with st.form("new_recurring_form"):
        new_title = st.text_input("タイトル（識別用）", placeholder="例: 月曜朝 - 憂鬱共感")
        new_genre = st.selectbox("ジャンル",
            ["おはよう", "昼休み", "夕方", "おやすみ", "深夜", "曜日", "月末月初", "天気", "その他"])
        new_content = st.text_area("投稿内容（ハッシュタグ含む）", height=200)
        new_category = st.selectbox("カテゴリ", ["evergreen", "seasonal", "trend"])
        if st.form_submit_button("💾 登録", type="primary"):
            if new_title and new_content:
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO marketing.buzz_references
                            (account_id, original_text, arrange_idea, genre, category, status)
                        VALUES (2, %s, %s, %s, %s, 'stocked')
                    """, (f"【定期投稿用テンプレ】{new_title}", new_content, new_genre, new_category))
                    conn.commit()
                    cur.close()
                    st.cache_data.clear()
                    st.success("登録しました！")
                    st.rerun()
                except Exception as e:
                    st.error(f"登録エラー: {e}")

# ── 競合分析タブ ──────────────────────────────────────────
def render_competitor_tab():
    st.markdown("### 🏆 競合アカウント分析")

    competitors = get_competitors()
    if not competitors.empty:
        # 最新データだけ取得（各競合の最新日付のレコード）
        latest = competitors.sort_values("measured_date", ascending=False).drop_duplicates(subset=["id"], keep="first")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### フォロワー数比較")
            if not latest.empty and "followers" in latest.columns:
                fig = go.Figure(go.Bar(
                    x=latest["username"], y=latest["followers"],
                    marker_color=["#4A90E2", "#FF6B35", "#27ae60", "#e74c3c", "#9b59b6"][:len(latest)],
                    text=latest["followers"].apply(lambda x: f"{x:,}" if pd.notna(x) else ""),
                    textposition="outside",
                ))
                fig.update_layout(
                    height=300, margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="white", plot_bgcolor="white",
                )
                st.plotly_chart(fig, use_container_width=True, key="comp_followers")

        with col2:
            st.markdown("#### フォロワー推移")
            if "measured_date" in competitors.columns:
                for comp_name in competitors["username"].unique():
                    comp_data = competitors[competitors["username"] == comp_name].sort_values("measured_date")
                    if not comp_data.empty and comp_data["followers"].notna().any():
                        st.line_chart(comp_data.set_index("measured_date")["followers"], height=250)
                        break
                else:
                    st.info("推移データ蓄積中...")

        st.divider()
        st.markdown("#### 競合一覧")
        st.dataframe(
            latest[["username", "display_name", "genre", "followers", "tweet_count", "avg_likes", "notes"]].rename(columns={
                "username": "アカウント", "display_name": "表示名", "genre": "ジャンル",
                "followers": "フォロワー", "tweet_count": "投稿数",
                "avg_likes": "平均いいね", "notes": "メモ",
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("競合アカウントを登録しましょう")

    st.divider()
    with st.expander("➕ 競合アカウントを追加"):
        comp_col1, comp_col2 = st.columns(2)
        with comp_col1:
            comp_username = st.text_input("ユーザー名", placeholder="@なし", key="comp_user")
            comp_display = st.text_input("表示名", key="comp_display")
        with comp_col2:
            comp_genre = st.selectbox("ジャンル", ["AI美女", "おもしろ系", "グラビア", "ニート系", "その他"], key="comp_genre")
            comp_notes = st.text_input("メモ", key="comp_notes")
        if st.button("💾 追加", key="comp_save"):
            if comp_username:
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO marketing.competitor_accounts
                            (username, display_name, genre, notes)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (platform, username) DO NOTHING
                    """, (comp_username, comp_display or None, comp_genre, comp_notes or None))
                    conn.commit()
                    cur.close()
                    st.success(f"@{comp_username} を追加しました")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"エラー: {e}")

# ════════════════════════════════════════════════════════════
# メイン
# ════════════════════════════════════════════════════════════
title_col, refresh_col = st.columns([5, 1])
with title_col:
    st.title("📊 SNS 運用ダッシュボード")
    st.caption(f"最終更新: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
with refresh_col:
    st.write("")
    if st.button("🔄 最新データに更新", use_container_width=True, type="primary"):
        # 先にキャッシュクリアして必ずX APIから最新を取る
        st.cache_data.clear()
        with st.spinner("X APIから最新データを取得中..."):
            try:
                conn = get_db()
                cur = conn.cursor()
                today = date.today()
                results = []
                for account_name, account_id in [("lumina", 1), ("myaku", 2)]:
                    # キャッシュバイパスで直接X APIを叩く
                    client = get_lumina_client() if account_name == "lumina" else get_myaku_client()
                    me = client.get_me(user_fields=["public_metrics"])
                    m = me.data.public_metrics
                    cur.execute("""
                        INSERT INTO marketing.daily_summary
                            (account_id, summary_date, posts_count, impressions, likes, followers, follows)
                        VALUES (%s, %s, %s, 0, 0, %s, %s)
                        ON CONFLICT (account_id, summary_date) DO UPDATE SET
                            posts_count = EXCLUDED.posts_count,
                            followers = EXCLUDED.followers,
                            follows = EXCLUDED.follows
                    """, (
                        account_id, today,
                        m["tweet_count"], m["followers_count"], m["following_count"]
                    ))
                    results.append(f"@{me.data.username}: {m['followers_count']:,}フォロワー")
                conn.commit()
                cur.close()
                st.success("✅ 更新完了！  " + " / ".join(results))
                st.rerun()
            except Exception as e:
                st.error(f"更新エラー: {e}")

tabs = st.tabs([
    "🌟 Lumina",
    "🫧 ミャクやん",
    "🔥 バズストック",
    "📅 定期投稿",
    "#️⃣ ハッシュタグ",
    "🏆 競合分析",
])

with tabs[0]:
    render_tab("lumina", 1, {
        "tweets": 90, "impressions": 5_000_000, "engagement": 50_000,
        "followers": 500, "likes": 1000, "retweets": 300,
        "clicks": 500, "following": 200,
    }, "lumina")

with tabs[1]:
    render_tab("myaku", 2, {
        "tweets": 150, "impressions": 1_000_000, "engagement": 30_000,
        "followers": 2000, "likes": 5000, "retweets": 1000,
        "clicks": 800, "following": 500,
    }, "myaku")

with tabs[2]:
    render_buzz_tab()

with tabs[3]:
    render_recurring_tab()

with tabs[4]:
    render_hashtag_tab()

with tabs[5]:
    render_competitor_tab()
