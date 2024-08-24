pub trait EnsureResultExt<T, E> {
    fn ensure_or<P>(self, predicate: P, error: E) -> Result<T, E>
    where
        P: FnOnce(&T) -> bool;

    fn ensure_or_else<F, P>(self, predicate: P, error: F) -> Result<T, E>
    where
        F: FnOnce(&T) -> E,
        P: FnOnce(&T) -> bool;
}

impl<T, E> EnsureResultExt<T, E> for Result<T, E> {
    fn ensure_or<P>(self, predicate: P, error: E) -> Result<T, E>
    where
        P: FnOnce(&T) -> bool,
    {
        self.and_then(|value| {
            if predicate(&value) {
                Ok(value)
            } else {
                Err(error)
            }
        })
    }

    fn ensure_or_else<F, P>(self, predicate: P, error: F) -> Result<T, E>
    where
        F: FnOnce(&T) -> E,
        P: FnOnce(&T) -> bool,
    {
        self.and_then(|value| {
            if predicate(&value) {
                Ok(value)
            } else {
                Err(error(&value))
            }
        })
    }
}

pub trait EnsureOptionExt<T> {
    fn ensure<P>(self, predicate: P) -> Option<T>
    where
        P: FnOnce(&T) -> bool;
}

impl<T> EnsureOptionExt<T> for Option<T> {
    fn ensure<P>(self, predicate: P) -> Option<T>
    where
        P: FnOnce(&T) -> bool,
    {
        self.and_then(|value| if predicate(&value) { Some(value) } else { None })
    }
}

pub trait PureResultExt<T, E> {
    fn pure(t: T) -> Result<T, E>;
}

impl<T, E> PureResultExt<T, E> for Result<T, E> {
    fn pure(t: T) -> Result<T, E> {
        Ok(t)
    }
}

pub trait PureOptionExt<T> {
    fn pure(t: T) -> Option<T>;
}

impl<T> PureOptionExt<T> for Option<T> {
    fn pure(t: T) -> Option<T> {
        Some(t)
    }
}
