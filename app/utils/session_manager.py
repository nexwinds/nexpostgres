from flask.sessions import SessionInterface, SessionMixin
from werkzeug.datastructures import CallbackDict
from datetime import datetime
import pickle
import uuid
from itsdangerous import Signer, BadSignature

from app.models.database import db, FlaskSession

class SQLAlchemySession(CallbackDict, SessionMixin):
    """Custom session class for SQLAlchemy-backed session."""
    
    def __init__(self, initial=None, sid=None, new=False):
        def on_update(self):
            self.modified = True
            
        CallbackDict.__init__(self, initial, on_update)
        self.sid = sid
        # Removed user_id for single-user mode
        self.new = new
        self.modified = False
        self.permanent = True  # Sessions are permanent by default
    
    def regenerate(self):
        """Regenerate session ID for security purposes."""
        self.sid = str(uuid.uuid4())
        self.modified = True

class SQLAlchemySessionInterface(SessionInterface):
    """Session interface that stores sessions in the database."""
    
    def open_session(self, app, request):
        cookie_name = app.config.get('SESSION_COOKIE_NAME', 'session')
        sid = request.cookies.get(cookie_name)
        
        if sid:
            # Validate the session id
            signer = Signer(app.secret_key)
            try:
                sid = signer.unsign(sid).decode()
                stored_session = FlaskSession.query.filter_by(id=sid).first()
                
                if stored_session and (stored_session.expiry is None or stored_session.expiry > datetime.utcnow()):
                    try:
                        data = pickle.loads(stored_session.session_data)
                        return SQLAlchemySession(data, sid=sid)
                    except Exception:
                        pass  # Create new session if data is invalid
            except BadSignature:
                pass
        
        # Create new session
        return SQLAlchemySession(sid=str(uuid.uuid4()), new=True)
    
    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        path = self.get_cookie_path(app)
        
        # Don't save empty unmodified sessions
        if not session and not session.modified:
            return
            
        # Set expiry time and session data
        expiry = self.get_expiration_time(app, session) if session.permanent else None
        session_data = pickle.dumps(dict(session))
        # Removed user_id handling for single-user mode
        
        # Clean up old sessions for this user
        # Removed user_id assignment for single-user mode
        FlaskSession.query.filter(
            # Removed user_id filtering for single-user mode
            FlaskSession.id != session.sid
        ).delete()
        
        # Update or create session record
        stored_session = FlaskSession.query.filter_by(id=session.sid).first()
        
        if stored_session:
            stored_session.session_data = session_data
            stored_session.expiry = expiry
            # Removed user_id assignment for single-user mode
        else:
            new_session = FlaskSession(
                id=session.sid,
                session_data=session_data,
                expiry=expiry,
                # Removed user_id for single-user mode
            )
            db.session.add(new_session)
        
        # Commit changes
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            
        # Set cookie if needed
        if not self.should_set_cookie(app, session):
            return
            
        # Get cookie settings
        cookie_name = app.config.get('SESSION_COOKIE_NAME', 'session')
        signed_sid = Signer(app.secret_key).sign(session.sid.encode()).decode()
        max_age = None
        
        if session.permanent and app.config.get('PERMANENT_SESSION_LIFETIME'):
            lifetime = app.config.get('PERMANENT_SESSION_LIFETIME')
            max_age = lifetime.total_seconds() if hasattr(lifetime, 'total_seconds') else 86400
            
        # Set cookie
        response.set_cookie(
            cookie_name,
            signed_sid,
            max_age=max_age,
            expires=expiry,
            httponly=self.get_cookie_httponly(app),
            domain=domain,
            path=path,
            secure=self.get_cookie_secure(app),
            samesite=self.get_cookie_samesite(app)
        )

def init_session(app):
    """Initialize the custom session interface."""
    app.session_interface = SQLAlchemySessionInterface()