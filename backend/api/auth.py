"""
api/auth.py — Auth routes (register, login, logout, me)
"""
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, render_template
from flask_login import login_user, logout_user, login_required, current_user
from backend.models import db, User, LoginSession

auth_bp = Blueprint("auth", __name__)


@auth_bp.post("/register")
def register():
    body = request.get_json(force=True)
    username = body.get("username", "").strip()
    email    = body.get("email", "").strip().lower()
    password = body.get("password", "")

    if not username or not email or not password:
        return jsonify({"error": "username, email, and password required"}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400
    if User.query.filter_by(username=username).first():
        return jsonify({"error": "Username already taken"}), 409
    if User.query.filter_by(email=email).first():
        return jsonify({"error": "Email already registered"}), 409

    user = User(username=username, email=email)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    login_user(user)
    return jsonify({"success": True, "user": user.to_dict()}), 201


@auth_bp.post("/login")
def login():
    body     = request.get_json(force=True)
    username = body.get("username", "").strip()
    password = body.get("password", "")

    user = User.query.filter_by(username=username).first()
    if not user or not user.check_password(password):
        return jsonify({"error": "Invalid credentials"}), 401

    login_user(user)
    user.last_login = datetime.now(timezone.utc)

    sess = LoginSession(
        user_id=user.id,
        ip_address=request.remote_addr,
        user_agent=request.user_agent.string[:300],
    )
    db.session.add(sess)
    db.session.commit()
    return jsonify({"success": True, "user": user.to_dict()})


@auth_bp.post("/logout")
@login_required
def logout():
    sess = (LoginSession.query
            .filter_by(user_id=current_user.id, logged_out=None)
            .order_by(LoginSession.logged_in.desc()).first())
    if sess:
        sess.logged_out = datetime.now(timezone.utc)
        db.session.commit()
    logout_user()
    return jsonify({"success": True})


@auth_bp.get("/me")
@login_required
def me():
    return jsonify(current_user.to_dict())


@auth_bp.get("/sessions")
@login_required
def sessions():
    data = (LoginSession.query
            .filter_by(user_id=current_user.id)
            .order_by(LoginSession.logged_in.desc())
            .limit(20).all())
    return jsonify([s.to_dict() for s in data])
