from flask import Flask, render_template, request, redirect, url_for
import sqlite3
import os

app = Flask(__name__)

DATABASE_PATH = '/data/media_database.db'


def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM unaccounted')
    movies = cur.fetchall()
    conn.close()
    return render_template('index.html', movies=movies)


@app.route('/edit/<int:id>', methods=['GET', 'POST'])
def edit(id):
    conn = get_db_connection()
    movie = conn.execute('SELECT * FROM unaccounted WHERE id = ?', (id,)).fetchone()

    if request.method == 'POST':
        new_symlink_folder = request.form['symlink_folder']
        new_symlink_filename = request.form['symlink_filename']

        # Delete the old symlink
        old_symlink_path = os.path.join(movie['symlink_top_folder'], movie['symlink_filename'])
        if os.path.islink(old_symlink_path):
            os.unlink(old_symlink_path)

        # Create the new symlink
        source_file_path = os.path.join(movie['src_dir'], movie['file_name'])
        new_symlink_path = os.path.join(new_symlink_folder, new_symlink_filename)
        os.symlink(source_file_path, new_symlink_path)

        # Update the database with new symlink information
        conn.execute('''
            UPDATE unaccounted
            SET symlink_top_folder = ?, symlink_filename = ?
            WHERE id = ?
        ''', (new_symlink_folder, new_symlink_filename, id))
        conn.commit()
        conn.close()
        return redirect(url_for('index'))

    conn.close()
    return render_template('edit.html', movie=movie)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
