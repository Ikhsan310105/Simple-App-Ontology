import os
from flask import Flask, render_template, request, redirect, url_for

from services.user_service import (
    init_user_indexes,
    list_users,
    create_user,
    delete_user,
    update_user_email,
    list_skills,
    get_user,
    replace_user_skills,
    list_user_matches,
    start_matching,
)
from services.neo4j_service import reset_neo4j_database

app = Flask(__name__)
app.config["_INIT_DONE"] = False

@app.before_request
def _ensure_indexes():
    if not app.config.get("_INIT_DONE"):
        try:
            init_user_indexes()
        except Exception:
            pass
        app.config["_INIT_DONE"] = True

# List users at root
@app.get("/")
def users_page():
    users = list_users(limit=500)
    skills = list_skills(limit=5000)
    return render_template("users.html", users=users, skills=skills)

# Create form
@app.get("/new")
def users_new():
    all_skills = list_skills(limit=50000)
    return render_template("user_form.html",
                           mode="create",
                           title="Tambah User",
                           all_skills=all_skills,
                           email_value="",
                           initial_skills=[])

# Handle create
@app.post("/create")
def users_create():
    email = request.form.get("email", "").strip()
    skills = request.form.getlist("skills")
    if email:
        create_user(email, [s for s in skills if s])
        start_matching()
    return redirect(url_for("users_page"))

# Edit form
@app.get("/<email>/edit")
def users_edit(email):
    user = get_user(email)
    if not user:
        return redirect(url_for("users_page"))
    all_skills = list_skills(limit=5000)
    return render_template("user_form.html",
                           mode="edit",
                           title="Edit User",
                           all_skills=all_skills,
                           email_value=user["email"],
                           initial_skills=user["skills"])

# Handle update (email + skills)
@app.post("/<old_email>/edit")
def users_update(old_email):
    new_email = request.form.get("email", "").strip()
    selected = [s for s in request.form.getlist("skills") if s]
    if new_email:
        if old_email != new_email:
            update_user_email(old_email, new_email)
        replace_user_skills(new_email, selected)
        start_matching()
    return redirect(url_for("users_page"))

# Delete user
@app.post("/<email>/delete")
def users_delete(email):
    if email:
        delete_user(email)
    return redirect(url_for("users_page"))

# View matches for a user
@app.get("/<email>/view")
def users_view(email):
    matches = list_user_matches(email)
    return render_template("user_view.html", email=email, matches=matches)

# Reset Database
@app.post("/reset")
def reset_and_rematch():
    try:
        reset_neo4j_database(drop_n10s_config=False)
        start_matching()
    except Exception as e:
        app.logger.exception("Reset/rematch failed: %s", e)
    return redirect(url_for("users_page"))

if __name__ == "__main__":
    try:
        init_user_indexes()
    except Exception:
        pass
    app.run(host="0.0.0.0", port=5000, debug=True)