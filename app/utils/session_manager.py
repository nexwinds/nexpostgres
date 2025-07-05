from flask.sessions import SessionInterface, SessionMixin
from flask_sqlalchemy import SQLAlchemy
from werkzeug.datastructures import CallbackDict
from datetime import datetime
import pickle
import uuid
from itsdangerous import Signer, BadSignature
from flask import current_app

from app.models.database import db, FlaskSession

class SQLAlchemySession(CallbackDict, SessionMixin):
    """Custom session class for SQLAlchemy-backed session."""
    
    def __init__(self, initial=None, sid=None, user_id=None, new=False):
        def on_update(self):
            self.modified = True
            
        CallbackDict.__init__(self, initial, on_update)
        self.sid = sid
        self.user_id = user_id
        self.new = new
        self.modified = False
        # Ensure session is marked as permanent by default
        self.permanent = True

class SQLAlchemySessionInterface(SessionInterface):
    """Session interface that stores sessions in the database."""
    
    def __init__(self):
        self.db = db
    
    def open_session(self, app, request):
        # Use the SESSION_COOKIE_NAME config value instead of attribute
        cookie_name = app.config.get('SESSION_COOKIE_NAME', 'session')
        sid = request.cookies.get(cookie_name)
        
        if sid:
            # Try to validate the session id
            signer = Signer(app.secret_key)
            try:
                sid_as_bytes = signer.unsign(sid)
                sid = sid_as_bytes.decode()
            except BadSignature:
                sid = None
                
        if sid:
            # Try to load the session from the database
            stored_session = FlaskSession.query.filter_by(id=sid).first()
            
            if stored_session:
                # Check expiry only if it exists
                if stored_session.expiry is None or stored_session.expiry > datetime.utcnow():
                    try:
                        data = pickle.loads(stored_session.data)
                        user_id = stored_session.user_id
                        return SQLAlchemySession(data, sid=sid, user_id=user_id)
                    except Exception as e:
                        print(f"Session loading error: {e}")
                        # Session data was invalid - create a new session
                        pass
        
        # Create a new session
        sid = str(uuid.uuid4())
        return SQLAlchemySession(sid=sid, new=True)
    
    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        path = self.get_cookie_path(app)
        
        # Don't save a session that's empty and not modified
        if not session and not session.modified:
            return
            
        # Calculate expiry time
        if session.permanent:
            expiry = self.get_expiration_time(app, session)
        else:
            expiry = None
            
        # Serialize the session data
        session_data = pickle.dumps(dict(session))
        
        # Extract user_id from session if it exists
        user_id = session.get('user_id')
        
        # If a user_id is set, delete any previous sessions for this user
        if user_id:
            # Update session user_id for tracking
            session.user_id = user_id
            
            # Delete old sessions for this user
            FlaskSession.query.filter(
                FlaskSession.user_id == user_id,
                FlaskSession.id != session.sid
            ).delete()
        
        # Get the existing session or create a new one
        stored_session = FlaskSession.query.filter_by(id=session.sid).first()
        
        if stored_session:
            stored_session.data = session_data
            stored_session.expiry = expiry
            stored_session.user_id = getattr(session, 'user_id', None)
        else:
            new_session = FlaskSession(
                id=session.sid,
                data=session_data,
                expiry=expiry,
                user_id=getattr(session, 'user_id', None)
            )
            db.session.add(new_session)
        
        # Commit changes
        try:
            db.session.commit()
        except Exception as e:
            print(f"Session save error: {e}")
            db.session.rollback()
            
        # Set the cookie
        if not self.should_set_cookie(app, session):
            return
            
        httponly = self.get_cookie_httponly(app)
        secure = self.get_cookie_secure(app)
        samesite = self.get_cookie_samesite(app)
        
        # Sign the session id for security
        signer = Signer(app.secret_key)
        signed_sid = signer.sign(session.sid.encode()).decode()
        
        # Use SESSION_COOKIE_NAME from config
        cookie_name = app.config.get('SESSION_COOKIE_NAME', 'session')
        
        # Set cookie with max_age to ensure it persists
        response.set_cookie(
            cookie_name,
            signed_sid,
            max_age=app.config.get('PERMANENT_SESSION_LIFETIME').total_seconds() if hasattr(app.config.get('PERMANENT_SESSION_LIFETIME'), 'total_seconds') else 86400,
            expires=expiry,
            httponly=httponly,
            domain=domain,
            path=path,
            secure=secure,
            samesite=samesite
        )

def init_session(app):
    """Initialize the custom session interface."""
    app.session_interface = SQLAlchemySessionInterface() 