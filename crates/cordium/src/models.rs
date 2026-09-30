use crate::{Error, Result, meta};
use std::time::Duration;

/// Exactly one resource name or cluster-wide UID. Strings convert to names.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Reference {
    /// A short or Space-qualified resource name.
    Name(String),
    /// A cluster-wide resource UID.
    Uid(String),
}
impl Reference {
    /// Constructs a reference by resource name; validation occurs when used.
    pub fn name(value: impl Into<String>) -> Self {
        Self::Name(value.into())
    }
    /// Constructs a reference by cluster-wide UID; validation occurs when used.
    pub fn uid(value: impl Into<String>) -> Self {
        Self::Uid(value.into())
    }
    pub(crate) fn object(&self) -> Result<meta::ObjectReference> {
        let mut r = meta::ObjectReference::default();
        match self {
            Self::Name(v) => {
                crate::error::nonempty(v, "resource name")?;
                r.name = v.clone();
            }
            Self::Uid(v) => {
                crate::error::nonempty(v, "resource UID")?;
                r.uid = v.clone();
            }
        }
        Ok(r)
    }
    pub(crate) fn get(&self) -> Result<meta::GetOptions> {
        let r = self.object()?;
        Ok(meta::GetOptions {
            name: r.name,
            uid: r.uid,
        })
    }
    pub(crate) fn delete(&self) -> Result<meta::DeleteOptions> {
        let r = self.object()?;
        Ok(meta::DeleteOptions {
            name: r.name,
            uid: r.uid,
        })
    }
}
impl From<String> for Reference {
    fn from(v: String) -> Self {
        Self::Name(v)
    }
}
impl From<&str> for Reference {
    fn from(v: &str) -> Self {
        Self::Name(v.into())
    }
}
impl From<&String> for Reference {
    fn from(v: &String) -> Self {
        Self::Name(v.clone())
    }
}
impl From<&Reference> for Reference {
    fn from(v: &Reference) -> Self {
        v.clone()
    }
}

/// A single server page. Page numbering starts at zero.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Page<T> {
    /// Resources returned in this page.
    pub items: Vec<T>,
    /// Server pagination metadata, including whether another page exists.
    pub info: meta::ListResponseMeta,
}
impl<T> Page<T> {
    pub(crate) fn new(items: Vec<T>, info: Option<meta::ListResponseMeta>) -> Self {
        Self {
            items,
            info: info.unwrap_or_default(),
        }
    }
}
/// Sort key used by resource lists.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OrderBy {
    /// Use the server default ordering.
    #[default]
    Default,
    /// Sort by resource name.
    Name,
    /// Sort by creation timestamp.
    CreatedAt,
}
/// Resource list filter. A single enum prevents conflicting filters.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ListFilter {
    /// Only resources in the given Space.
    Space(Reference),
    /// Only Workspaces of the given Template.
    Template(Reference),
    /// Only snapshots of the given Workspace.
    Workspace(Reference),
}
/// Which relationship selects Spaces in a list.
#[derive(Clone, Copy, Debug, Default)]
pub enum SpaceMode {
    /// Spaces created by the authenticated user.
    #[default]
    CreatedBy,
    /// Spaces in which the authenticated user is a member.
    Member,
}
/// Pagination and filter configuration. Construct with `Default` and builder methods.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ListOptions {
    /// Zero-based page index.
    pub page: u32,
    /// Requested page size. Zero selects the server default.
    pub page_size: u32,
    /// Sort key.
    pub order_by: OrderBy,
    /// Reverse the sorting direction.
    pub descending: bool,
    /// Optional collection-specific filter. Unsupported filters are rejected.
    pub filter: Option<ListFilter>,
    /// Relationship used only when listing Spaces.
    pub space_mode: SpaceMode,
}
impl ListOptions {
    /// Starts a list with server defaults.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the page size; zero keeps the server default.
    pub fn page_size(mut self, n: u32) -> Self {
        self.page_size = n;
        self
    }
    /// Sets the zero-based page.
    pub fn page(mut self, n: u32) -> Self {
        self.page = n;
        self
    }
    /// Filters by Space.
    pub fn in_space(mut self, r: impl Into<Reference>) -> Self {
        self.filter = Some(ListFilter::Space(r.into()));
        self
    }
    /// Filters Workspaces by Template.
    pub fn of_template(mut self, r: impl Into<Reference>) -> Self {
        self.filter = Some(ListFilter::Template(r.into()));
        self
    }
    /// Filters snapshots by source Workspace.
    pub fn of_workspace(mut self, r: impl Into<Reference>) -> Self {
        self.filter = Some(ListFilter::Workspace(r.into()));
        self
    }
    /// Sets sort key and direction.
    pub fn order(mut self, key: OrderBy, descending: bool) -> Self {
        self.order_by = key;
        self.descending = descending;
        self
    }
    /// Includes Spaces where the user holds a membership.
    pub fn member_spaces(mut self) -> Self {
        self.space_mode = SpaceMode::Member;
        self
    }
    pub(crate) fn common(&self) -> meta::CommonListOptions {
        meta::CommonListOptions {
            page: self.page,
            items_per_page: self.page_size,
            order_by: if self.order_by == OrderBy::Default && !self.descending {
                None
            } else {
                Some(meta::common_list_options::OrderBy {
                    r#type: match self.order_by {
                        OrderBy::Default => 0,
                        OrderBy::Name => 1,
                        OrderBy::CreatedAt => 2,
                    },
                    mode: if self.descending { 2 } else { 1 },
                })
            },
        }
    }
    pub(crate) fn space(&self) -> Result<Option<meta::ObjectReference>> {
        match &self.filter {
            None => Ok(None),
            Some(ListFilter::Space(r)) => Ok(Some(r.object()?)),
            _ => Err(Error::InvalidArgument(
                "this collection only supports Space filters".into(),
            )),
        }
    }
    pub(crate) fn unfiltered(&self) -> Result<()> {
        if self.filter.is_some() {
            return Err(Error::InvalidArgument(
                "this collection does not support filters".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn next(&mut self, info: meta::ListResponseMeta, empty: bool) -> Result<bool> {
        if !info.has_more {
            return Ok(false);
        }
        if empty || info.page < self.page {
            return Err(Error::Protocol("pagination did not advance".into()));
        }
        self.page = info
            .page
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("page index overflow".into()))?;
        Ok(true)
    }
}
/// Total deadline and polling interval for composite operations.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct WaitOptions {
    /// Total time including all RPCs. `None` leaves cancellation to the caller.
    pub timeout: Option<Duration>,
    /// Delay between status requests.
    pub poll_interval: Duration,
}
impl Default for WaitOptions {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(300)),
            poll_interval: Duration::from_millis(500),
        }
    }
}
impl WaitOptions {
    /// Sets the total deadline, or disables it with `None`.
    pub fn timeout(mut self, t: Option<Duration>) -> Self {
        self.timeout = t;
        self
    }
    /// Sets the polling interval.
    pub fn poll_interval(mut self, t: Duration) -> Self {
        self.poll_interval = t;
        self
    }
    pub(crate) fn validate(self) -> Result<Self> {
        if self.poll_interval.is_zero() || self.timeout.is_some_and(|t| t.is_zero()) {
            return Err(Error::InvalidArgument(
                "timeouts and polling intervals must be positive".into(),
            ));
        }
        Ok(self)
    }
}
