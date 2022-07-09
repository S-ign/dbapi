package function

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/S-ign/httputils"
	"github.com/S-ign/vaultutils"
	"github.com/gofrs/uuid"
	"github.com/jackc/pgx/v4"
	handler "github.com/openfaas/templates-sdk/go-http"
)

// Creater handles inserting a single row into a database
type Creater interface {
	create(db *pgx.Conn, table string) error
}

type registrationDetail struct {
	Name    string   `json:"name"`
	Phone   string   `json:"phone"`
	Members []string `json:"members"`
	Shirt   []string `json:"shirt"`
	Club    []string `json:"club"`
}

func getRegistrationDetail(db *pgx.Conn) ([]registrationDetail, error) {
	var rd registrationDetail
	var rdList []registrationDetail
	exec := fmt.Sprintf(`
	select c.name, c.phone, t.members, t.shirt, t1.club
	from customer c
	inner join salesorder s on
	s.customerId = c.customerId
	inner join (
			select s.salesorderid, string_agg(p.name, '\n' order by p.name) as members,
					string_agg(oi.name, '\n' order by p.name) as shirt
			from participant p
			inner join purchase pu
			on pu.purchaseId = p.purchaseId
			inner join salesorder s 
			on s.salesorderId = pu.salesorderid
			inner join participant_option po 
	on po.participantId = p.participantId
	inner join option_item oi
	on oi.optionItemsId = po.optionItemsId 
	inner join category_option co
	on co.categoryOptionsId = oi.categoryOptionsId
	and co.name = 'T-Shirt'
	group by s.salesorderid
	) t
	on t.salesorderId = s.salesorderId
	inner join (
			select s.salesorderid, string_agg(p.name, '\n' order by p.name) as members,
					string_agg(oi.name, '\n' order by p.name) as club
			from participant p
			inner join purchase pu
			on pu.purchaseId = p.purchaseId
			inner join salesorder s 
			on s.salesorderId = pu.salesorderid
			inner join participant_option po 
	on po.participantId = p.participantId
	inner join option_item oi
	on oi.optionItemsId = po.optionItemsId 
	inner join category_option co
	on co.categoryOptionsId = oi.categoryOptionsId
	and co.name = 'Dexterity'
	group by s.salesorderid
	) t1
	on t1.salesorderId = s.salesorderId
	`)
	rows, err := db.Query(context.Background(), exec)
	if err != nil {
		return nil, fmt.Errorf("getRegistrationDetail query err: %v", err)
	}
	for rows.Next() {
		var members string
		var shirt string
		var club string
		rows.Scan(&rd.Name, &rd.Phone, &members, &shirt, &club)
		if err != nil {
			return nil, fmt.Errorf("getRegistrationDetail scan err: %v", err)
		}
		rd.Members = strings.Split(members, "\\n")
		rd.Shirt = strings.Split(shirt, "\\n")
		rd.Club = strings.Split(club, "\\n")
		rdList = append(rdList, rd)
	}

	return rdList, nil
}

type registrationBreakdown struct {
	SoloRegistration     int    `json:"soloregistration"`
	SoloCollected        string `json:"solocollected"`
	TwosomeRegistration  int    `json:"twosomeregistration"`
	TwosomeCollected     string `json:"twosomecollected"`
	FoursomeRegistration int    `json:"foursomeregistration"`
	FoursomeCollected    string `json:"foursomecollected"`
}

func (rb *registrationBreakdown) getRegistrationBreakdown(db *pgx.Conn) error {
	exec := fmt.Sprintf(`
	select
	sum(case when productName = 'Solo Registration' then qty else 0 end) as "Solo Registration",
	sum(case when productName = 'Solo Registration' then price else cast(0 as money) end) as "Solo Collected",
	sum(case when productName = 'Twosome Registration' then qty else 0 end) as "Twosome Registration",
	sum(case when productName = 'Twosome Registration' then price else cast(0 as money) end) as "Twosome Collected",
	sum(case when productName = 'Foursome Registration' then qty else 0 end) as "Foursome Registration",
	sum(case when productName = 'Solo Registration' then price else cast(0 as money) end) as "Foursome Collected"
	from participant pa
	inner join purchase pu on
	pa.purchaseId = pu.purchaseId
	`)
	row := db.QueryRow(context.Background(), exec)
	err := row.Scan(
		&rb.SoloRegistration, &rb.SoloCollected,
		&rb.TwosomeRegistration, &rb.TwosomeCollected,
		&rb.FoursomeRegistration, &rb.FoursomeCollected,
	)
	if err != nil {
		return fmt.Errorf("getRegistrationBreakdown scan err: %v", err)
	}

	return nil
}

type dashboardSummary struct {
	Participants int    `json:"participants"`
	Collected    string `json:"collected"`
}

func (ds *dashboardSummary) getDashboardSummary(db *pgx.Conn) error {
	// Overall Summary
	exec := fmt.Sprintf(`
	select count(qty) as Participants, sum(price) as Collected
	from participant pa
	inner join purchase pu on
	pa.purchaseId = pu.purchaseId
	`)
	row := db.QueryRow(context.Background(), exec)
	err := row.Scan(&ds.Participants, &ds.Collected)
	if err != nil {
		return fmt.Errorf("getDashboardSummary scan err: %v", err)
	}

	return nil
}

type registrationSummary struct {
	SoloRegistration     int `json:"soloregistration"`
	TwosomeRegistration  int `json:"twosomeregistration"`
	FoursomeRegistration int `json:"foursomeregistration"`
}

func (rs *registrationSummary) getRegistrationSummary(db *pgx.Conn) error {
	// Registration Summary
	exec := fmt.Sprintf(`
	select
	sum(case when productName = 'Solo Registration' then qty else 0 end) as "Solo Registration",
	sum(case when productName = 'Twosome Registration' then qty else 0 end) as "Twosome Registration",
	sum(case when productName = 'Foursome Registration' then qty else 0 end) as "Foursome Registration"
	from participant pa
	inner join purchase pu on
	pa.purchaseId = pu.purchaseId
	`)
	row := db.QueryRow(context.Background(), exec)
	err := row.Scan(&rs.SoloRegistration, &rs.TwosomeRegistration, &rs.FoursomeRegistration)
	if err != nil {
		return fmt.Errorf("getRegistrationSummary scan err: %v", err)
	}

	return nil
}

type shirtSummary struct {
	Small   int `json:"small"`
	Medium  int `json:"medium"`
	Large   int `json:"large"`
	XLarge  int `json:"xlarge"`
	XXLarge int `json:"xxlarge"`
}

func (ss *shirtSummary) getShirtSummary(db *pgx.Conn) error {
	// Shirt Summary
	exec := fmt.Sprintf(`
	select
	count(case when Name = 'SMALL' then 1 end) as "SMALL",
	count(case when Name = 'MEDIUM' then 1 end) as "MEDIUM",
	count(case when Name = 'LARGE' then 1 end) as "LARGE",
	count(case when Name = 'X-LARGE' then 1 end) as "X-LARGE",
	count(case when Name = '2X-LARGE' then 1 end) as "2X-LARGE"
	from participant_option po
	inner join option_item oi
	on oi.optionitemsid = po.optionitemsid
	`)
	row := db.QueryRow(context.Background(), exec)
	err := row.Scan(&ss.Small, &ss.Medium, &ss.Large, &ss.XLarge, &ss.XXLarge)
	if err != nil {
		return fmt.Errorf("getShirtSummary scan err: %v", err)
	}

	return nil
}

type clubSummary struct {
	LeftHanded  int `json:"lefthanded"`
	RightHanded int `json:"righthanded"`
}

func (cs *clubSummary) getClubSummary(db *pgx.Conn) error {
	// Club Summary
	exec := fmt.Sprintf(`
	select
	count(case when Name = 'LEFT-HANDED' then 1 end) as "LEFT-HANDED",
	count(case when Name = 'RIGHT-HANDED' then 1 end) as "RIGHT-HANDED"
	from participant_option po
	inner join option_item oi
	on oi.optionitemsid = po.optionitemsid
	`)
	row := db.QueryRow(context.Background(), exec)
	err := row.Scan(&cs.LeftHanded, &cs.RightHanded)
	if err != nil {
		return fmt.Errorf("getClubSummary scan err: %v", err)
	}

	return nil
}

type golfer struct {
	Name      string `json:"name"`
	ShirtSize string `json:"shirtsize"`
	Dexterity string `json:"dexterity"`
}

type registration struct {
	OrderDate  string   `json:"orderdate"`
	SessionID  string   `json:"sessionid"`
	PricingID  string   `json:"pricingid"`
	GolferInfo []golfer `json:"golferinfo"`
}

func (r registration) create(db *pgx.Conn) error {
	var shoppingorderid int
	var sessionid string
	var orderdate time.Time
	var shoppingcartid int
	var cartparticipantid int

	exec := fmt.Sprintf("select * from shopping_order where sessionid = $1")
	err := db.QueryRow(context.Background(), exec, r.SessionID).Scan(&shoppingorderid, &orderdate, &sessionid)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		exec = fmt.Sprintf("insert into shopping_order(orderdate, sessionid) values($1, $2) returning shoppingorderid")
		err = db.QueryRow(context.Background(), exec, r.OrderDate, r.SessionID).Scan(&shoppingorderid)
		if err != nil {
			return fmt.Errorf("shopping_order: %v", err)
		}

	case err != nil:
		return fmt.Errorf("select shopping_order: %v", err)
	}

	qty := 1
	exec = fmt.Sprintf(`
	insert into shopping_cart(shoppingorderid, pricingid, qty)
	values ($1, $2, $3) returning shoppingcartid`)
	err = db.QueryRow(context.Background(), exec, shoppingorderid, r.PricingID, qty).Scan(&shoppingcartid)
	if err != nil {
		return fmt.Errorf("shopping_cart: %v", err)
	}

	golfers := len(r.GolferInfo)
	if golfers > 4 {
		return fmt.Errorf("error: incorrect number of golfers")
	}
	if golfers == 3 {
		return fmt.Errorf("error: incorrect number of golfers")
	}
	if golfers <= 0 {
		return fmt.Errorf("error: incorrect number of golfers")
	}

	//var cartparticipantid int
	for _, g := range r.GolferInfo {
		exec := fmt.Sprintf(`
		insert into cart_participant(shoppingcartid, name)
		values ($1, $2) returning cartparticipantid`)
		err = db.QueryRow(context.Background(), exec, shoppingcartid, g.Name).Scan(&cartparticipantid)
		if err != nil {
			return fmt.Errorf("cart_participant: %v", err)
		}

		shirtsize, err := strconv.Atoi(g.ShirtSize)
		if err != nil {
			return err
		}
		exec = fmt.Sprintf(`
		insert into cart_participant_option(cartparticipantid, optionitemsid)
		values ($1, $2)`)
		rows, err := db.Query(context.Background(), exec, cartparticipantid, shirtsize)
		if err != nil {
			return fmt.Errorf("cart_participant_option: shirtsize: %v", err)
		}
		for rows.Next() {
			rows.Close()
		}

		if g.Dexterity != "" {
			dexterity, err := strconv.Atoi(g.Dexterity)
			if err != nil {
				return err
			}
			if dexterity > 5 && dexterity < 8 {
				exec = fmt.Sprintf(`
				insert into cart_participant_option(cartparticipantid, optionitemsid)
				values ($1, $2)`)
				rows, err = db.Query(context.Background(), exec, cartparticipantid, dexterity)
				if err != nil {
					return fmt.Errorf("cart_participant_option: dexterity: %v", err)
				}
				for rows.Next() {
					rows.Close()
				}
			}
		}
	}

	return nil
}

type cart_participant struct {
	CartParticipantID string `json:"cartparticipantid"` //int
	ShoppingCartID    string `json:"shoppingcartid"`    //int
	Name              string `json:"name"`
}

func updateCartParticipant(db *pgx.Conn, d Data, i cart_participant) error {

	// convert struct to map
	imap := make(map[string]string)
	m, err := json.Marshal(i)
	if err != nil {
		return err
	}
	json.Unmarshal(m, &imap)

	exec, err := updateStringBuilder(d.Table, d.Update.SetFields, d.Update.SetValues, imap)
	_, err = db.Exec(context.Background(), exec)
	return fmt.Errorf("updatesalesorder: %v, sql string: %v", err.Error(), exec)
}

func (c cart_participant) create(db *pgx.Conn, table string) (int, error) {
	var cartparticipantid int
	exec := fmt.Sprintf("insert into %v(shoppingcartid, name) values($1, $2) returning cartparticipantid", table)
	err := db.QueryRow(context.Background(), exec, c.ShoppingCartID, c.Name).Scan(&cartparticipantid)
	return cartparticipantid, err
}

func (c *cart_participant) readall(db *pgx.Conn, table string) ([]cart_participant, error) {
	var so cart_participant
	var sl []cart_participant
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&c.ShoppingCartID, &c.Name)
		if err != nil {
			return nil, err
		}
		sl = append(sl, so)
	}
	return sl, nil
}

func (c *cart_participant) read(db *pgx.Conn, table, field, value string) ([]cart_participant, error) {
	var cp cart_participant
	var cl []cart_participant
	var cpID int
	var scID int
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&cpID, &scID, &cp.Name)
		if err != nil {
			return nil, err
		}
		cp.CartParticipantID = strconv.Itoa(cpID)
		cp.ShoppingCartID = strconv.Itoa(scID)
		cl = append(cl, cp)
	}
	return cl, nil
}

func (c *cart_participant) del(db *pgx.Conn, table, field, value string) error {
	exec := fmt.Sprintf("delete from %v where %v=$1", table, field)
	_, err := db.Exec(context.Background(), exec, value)
	return err
}

type category_options struct {
	ID                int    `json:"id"`
	PackageCategoryID int    `json:"packagecategoryid"`
	Name              string `json:"name"`
}

func (c *category_options) readall(db *pgx.Conn, table string) ([]category_options, error) {
	var co category_options
	var cl []category_options
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&co.ID, &co.PackageCategoryID, &co.Name)
		if err != nil {
			return nil, err
		}
		cl = append(cl, co)
	}
	return cl, nil
}

func (c *category_options) read(db *pgx.Conn, table, field, value string) ([]category_options, error) {
	var co category_options
	var cl []category_options
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&co.ID, &co.PackageCategoryID, &co.Name)
		if err != nil {
			return nil, err
		}
		cl = append(cl, co)
	}
	return cl, nil
}

type salesorder struct {
	SalesOrderID int    `json:"salesorderid"`
	OrderDate    string `json:"orderdate"`  // conv to time.Time
	CustomerID   string `json:"customerid"` // conv to int
	PaymentID    string `json:"paymentid"`
	InvoiceNo    string `json:"invoiceno"`
}

func (s *salesorder) Normalize() {
	strconv.Atoi(s.CustomerID)
}

func updateSalesOrder(db *pgx.Conn, d Data, i salesorder) error {
	i.Normalize()

	// convert struct to map
	imap := make(map[string]string)
	m, err := json.Marshal(i)
	if err != nil {
		return err
	}
	json.Unmarshal(m, &imap)

	exec, err := updateStringBuilder(d.Table, d.Update.SetFields, d.Update.SetValues, imap)
	_, err = db.Exec(context.Background(), exec)
	return fmt.Errorf("updatesalesorder: %v, sql string: %v", err.Error(), exec)
}

func (s salesorder) create(db *pgx.Conn, table string) error {
	s.Normalize()
	exec := fmt.Sprintf("insert into %v(salesorderid, orderdate, customerid, paymentid, invoiceno) values($1, $2, $3, $4, $5)", table)
	_, err := db.Exec(context.Background(), exec, s.SalesOrderID, s.OrderDate, s.CustomerID, s.PaymentID, s.InvoiceNo)
	return err
}

func (s *salesorder) readall(db *pgx.Conn, table string) ([]salesorder, error) {
	s.Normalize()
	var so salesorder
	var sl []salesorder
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&so.SalesOrderID, &so.OrderDate, &so.CustomerID, &so.PaymentID, &so.InvoiceNo)
		if err != nil {
			return nil, err
		}
		sl = append(sl, so)
	}
	return sl, nil
}

func (s *salesorder) read(db *pgx.Conn, table, field, value string) ([]salesorder, error) {
	s.Normalize()
	var so salesorder
	var sl []salesorder
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&so.SalesOrderID, &so.OrderDate, &so.CustomerID, &so.PaymentID, &so.InvoiceNo)
		if err != nil {
			return nil, err
		}
		sl = append(sl, so)
	}
	return sl, nil
}

type shopping_order struct {
	ShoppingOrderID string `json:"shoppingorderid"` //int
	OrderDate       string `json:"orderdate"`
	SessionID       string `json:"sessionid"`
}

func updateShoppingOrder(db *pgx.Conn, d Data, i shopping_order) error {
	// convert struct to map
	imap := make(map[string]string)
	m, err := json.Marshal(i)
	if err != nil {
		return err
	}
	json.Unmarshal(m, &imap)

	exec, err := updateStringBuilder(d.Table, d.Update.SetFields, d.Update.SetValues, imap)
	_, err = db.Exec(context.Background(), exec)
	return err
}

func (s shopping_order) create(db *pgx.Conn, table string) (int, error) {
	var shoppingorderid int
	exec := fmt.Sprintf("insert into %v(orderdate, sessionid) values($1, $2) returning shoppingorderid", table)
	err := db.QueryRow(context.Background(), exec, s.OrderDate, s.SessionID).Scan(&shoppingorderid)
	return shoppingorderid, err
}

func (s *shopping_order) del(db *pgx.Conn, table, field, value string) error {
	exec := fmt.Sprintf("delete from %v where %v=$1", table, field)
	_, err := db.Exec(context.Background(), exec, value)
	return err
}

func (s *shopping_order) readall(db *pgx.Conn, table string) ([]shopping_order, error) {
	var so shopping_order
	var sl []shopping_order
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&so.ShoppingOrderID, &so.OrderDate, &so.SessionID)
		if err != nil {
			return nil, err
		}
		sl = append(sl, so)
	}
	return sl, nil
}

func (s *shopping_order) read(db *pgx.Conn, table, field, value string) ([]shopping_order, error) {
	var so shopping_order
	var sl []shopping_order
	var shoppingorderid int
	var t time.Time
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&shoppingorderid, &t, &so.SessionID)
		if err != nil {
			return nil, err
		}
		so.ShoppingOrderID = strconv.Itoa(shoppingorderid)
		so.OrderDate = t.String()
		sl = append(sl, so)
	}
	return sl, nil
}

type organization struct {
	ID       uuid.UUID `json:"id"`
	Name     string    `json:"name"`
	Address  string    `json:"address"`
	City     string    `json:"city"`
	State    string    `json:"state"`
	PostCode string    `json:"postcode"`
	IsActive bool      `json:"isactive"`
}

func (o *organization) readall(db *pgx.Conn, table string) ([]organization, error) {
	var or organization
	var oa []organization
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&or.ID, &or.Name, &or.Address, &or.City, &or.State, &or.PostCode, &or.IsActive)
		if err != nil {
			return nil, err
		}
		oa = append(oa, or)
	}
	return oa, nil
}

func (o *organization) read(db *pgx.Conn, table, field, value string) ([]organization, error) {
	var or organization
	var oa []organization
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&or.ID, &or.Name, &or.Address, &or.City, &or.State, &or.PostCode, &or.IsActive)
		if err != nil {
			return nil, err
		}
		oa = append(oa, or)
	}
	return oa, nil
}

type event struct {
	ID             int       `json:"id"`
	OrganizationID uuid.UUID `json:"organizationid"`
	Name           string    `json:"name"`
	Location       string    `json:"location"`
	Capacity       int       `json:"capacity"`
	StartsOn       time.Time `json:"startson"`
	EndsOn         time.Time `json:"endson"`
}

func (e *event) readall(db *pgx.Conn, table string) ([]event, error) {
	var ev event
	var el []event
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&ev.ID, &ev.OrganizationID, &ev.Name, &ev.Location, &ev.Capacity, &ev.StartsOn, &ev.EndsOn)
		if err != nil {
			return nil, err
		}
		el = append(el, ev)
	}
	return el, nil
}

func (e *event) read(db *pgx.Conn, table, field, value string) ([]event, error) {
	var ev event
	var el []event
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&ev.ID, &ev.OrganizationID, &ev.Name, &ev.Location, &ev.Capacity, &ev.StartsOn, &ev.EndsOn)
		if err != nil {
			return nil, err
		}
		el = append(el, ev)
	}
	return el, nil
}

type payment_provider struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

func (p *payment_provider) readall(db *pgx.Conn, table string) ([]payment_provider, error) {
	var pp payment_provider
	var pl []payment_provider
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pp.ID, &pp.Name)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pp)
	}
	return pl, nil
}

func (p *payment_provider) read(db *pgx.Conn, table, field, value string) ([]payment_provider, error) {
	var pp payment_provider
	var pl []payment_provider
	query := fmt.Sprintf("select * from %v, where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pp.ID, &pp.Name)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pp)
	}
	return pl, nil
}

type customer struct {
	ID             int       `json:"id"`
	OrganizationID uuid.UUID `json:"organizationid"`
	Name           string    `json:"name"`
	Email          string    `json:"email"`
	Phone          string    `json:"phone"`
}

func (c *customer) readall(db *pgx.Conn, table string) ([]customer, error) {
	var cu customer
	var cl []customer
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&cu.ID, &cu.OrganizationID, &cu.Name, &cu.Email, &cu.Phone)
		if err != nil {
			return nil, err
		}
		cl = append(cl, cu)
	}
	return cl, nil
}

func (c *customer) read(db *pgx.Conn, table, field, value string) ([]customer, error) {
	var cu customer
	var cl []customer
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&cu.ID, &cu.OrganizationID, &cu.Name, &cu.Email, &cu.Phone)
		if err != nil {
			return nil, err
		}
		cl = append(cl, cu)
	}
	return cl, nil
}

type _package struct {
	ID                int    `json:"id"`
	EventID           int    `json:"eventid"`
	ProductID         string `json:"productid"`
	PackageCategoryID int    `json:"packagecategoryid"`
	Name              string `json:"name"`
	Description       string `json:"description"`
}

func (p *_package) readall(db *pgx.Conn, table string) ([]_package, error) {
	var pa _package
	var pl []_package
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pa.ID, &pa.EventID, &pa.ProductID, &pa.PackageCategoryID, &pa.Name, &pa.Description)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pa)
	}
	return pl, nil
}

type product struct {
	PaymentProviderID uuid.UUID `json:"paymentproviderid"`
}

func (p *product) readall(db *pgx.Conn, table string) ([]product, error) {
	var pr product
	var pl []product
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pr.PaymentProviderID)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pr)
	}
	return pl, nil
}

type option_items struct {
	ID                int    `json:"id"`
	CategoryOptionsID int    `json:"categoryoptionsid"`
	Name              string `json:"name"`
}

func (o *option_items) readall(db *pgx.Conn, table string) ([]option_items, error) {
	var oi option_items
	var ol []option_items
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&oi.ID, &oi.CategoryOptionsID, &oi.Name)
		if err != nil {
			return nil, err
		}
		ol = append(ol, oi)
	}
	return ol, nil
}

func (o *option_items) read(db *pgx.Conn, table, field, value string) ([]option_items, error) {
	var oi option_items
	var ol []option_items
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&oi.ID, &oi.CategoryOptionsID, &oi.Name)
		if err != nil {
			return nil, err
		}
		ol = append(ol, oi)
	}
	return ol, nil
}

type pricing struct {
	PricingID string `json:"pricingid"`
	ProductID string `json:"productid"`
}

func (p *pricing) readall(db *pgx.Conn, table string) ([]pricing, error) {
	var pr pricing
	var pl []pricing
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pr.PricingID, &pr.ProductID)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pr)
	}
	return pl, nil
}

func (p *pricing) read(db *pgx.Conn, table, field, value string) ([]pricing, error) {
	var pr pricing
	var pl []pricing
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pr.PricingID, &pr.ProductID)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pr)
	}
	return pl, nil
}

type purchase struct {
	ID          int    `json:"id"`
	OrderID     int    `json:"orderid"`
	Qty         int    `json:"qty"`
	ProductName string `json:"productname"`
	Description string `json:"description"`
	Price       string `json:"price"`
}

func (p *purchase) readall(db *pgx.Conn, table string) ([]purchase, error) {
	var pu purchase
	var pl []purchase
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pu.ID, &pu.OrderID, &pu.Qty, &pu.ProductName, &pu.Description, &pu.Price)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pu)
	}
	return pl, nil
}

func (p *purchase) read(db *pgx.Conn, table, field, value string) ([]purchase, error) {
	var pu purchase
	var pl []purchase
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pu.ID, &pu.OrderID, &pu.Qty, &pu.ProductName, &pu.Description, &pu.Price)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pu)
	}
	return pl, nil
}

type participant struct {
	ParticipantID string `json:"participantid"` // conv to int
	PurchaseID    string `json:"purshaseid"`    // conv to int
	Name          string `json:"name"`
}

func (p participant) create(db *pgx.Conn, table string) error {
	puid, _ := strconv.Atoi(p.PurchaseID)
	exec := fmt.Sprintf("insert into %v(purchaseid, name) values($1, $2)", table)
	_, err := db.Exec(context.Background(), exec, puid, p.Name)
	return err
}

func (p *participant) readall(db *pgx.Conn, table string) ([]participant, error) {
	var pa participant
	var pl []participant
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pa.ParticipantID, &pa.PurchaseID, &pa.Name)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pa)
	}
	return pl, nil
}

func (p *participant) read(db *pgx.Conn, table, field, value string) ([]participant, error) {
	var pa participant
	var pl []participant
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&pa.ParticipantID, &pa.PurchaseID, &pa.Name)
		if err != nil {
			return nil, err
		}
		pl = append(pl, pa)
	}
	return pl, nil
}

type participant_options struct {
	ID            int `json:"id"`
	ParticipantID int `json:"participantid"`
	OptionItemsID int `json:"optionitemsid"`
}

func (p *participant_options) readall(db *pgx.Conn, table string) ([]participant_options, error) {
	var po participant_options
	var pl []participant_options
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&po.ID, &po.ParticipantID, &po.OptionItemsID)
		if err != nil {
			return nil, err
		}
		pl = append(pl, po)
	}
	return pl, nil
}

func (p *participant_options) read(db *pgx.Conn, table, field, value string) ([]participant_options, error) {
	var po participant_options
	var pl []participant_options
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&po.ID, &po.ParticipantID, &po.OptionItemsID)
		if err != nil {
			return nil, err
		}
		pl = append(pl, po)
	}
	return pl, nil
}

type shopping_cart struct {
	ShoppingCartID  string `json:"shoppingcartid"`  //conv to int
	ShoppingOrderID string `json:"shoppingorderid"` //conv to int
	PricingID       string `json:"pricingid"`
	Qty             string `json:"qty"` //conv to int
}

// UpdateShoppingCart takes two shopping_cart object Unmarshalled from JSON
// into Data struct's Update.Identifier and Update.UpdateField, these objects
// are used to identify the row needed to be updated and to Update the field
// required.
func updateShoppingCart(db *pgx.Conn, d Data, i shopping_cart) error {

	// convert struct to map
	imap := make(map[string]string)
	m, err := json.Marshal(i)
	if err != nil {
		return err
	}
	json.Unmarshal(m, &imap)

	exec, err := updateStringBuilder(d.Table, d.Update.SetFields, d.Update.SetValues, imap)
	_, err = db.Exec(context.Background(), exec)
	return err
}

func (s shopping_cart) create(db *pgx.Conn, table string) (int, error) {
	var shoppingcartid int
	exec := fmt.Sprintf("insert into %v(shoppingorderid, pricingid, qty) values($1, $2, $3) returning shoppingcartid", table)
	err := db.QueryRow(context.Background(), exec, s.ShoppingOrderID, s.PricingID, s.Qty).Scan(&shoppingcartid)
	return shoppingcartid, err
}

func (s *shopping_cart) readall(db *pgx.Conn, table string) ([]shopping_cart, error) {
	var sc shopping_cart
	var sl []shopping_cart
	var scid int
	var soid int
	var qty int
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&scid, &soid, &sc.PricingID, &qty)
		if err != nil {
			return nil, err
		}
		sc.ShoppingCartID = strconv.Itoa(scid)
		sc.ShoppingOrderID = strconv.Itoa(soid)
		sc.Qty = strconv.Itoa(qty)
		sl = append(sl, sc)
	}
	return sl, nil
}

func (s *shopping_cart) read(db *pgx.Conn, table, field, value string) ([]shopping_cart, error) {
	var sc shopping_cart
	var sl []shopping_cart
	var scid int
	var soid int
	var qty int
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&scid, &soid, &sc.PricingID, &qty)
		if err != nil {
			return nil, err
		}
		sc.ShoppingCartID = strconv.Itoa(scid)
		sc.ShoppingOrderID = strconv.Itoa(soid)
		sc.Qty = strconv.Itoa(qty)
		sl = append(sl, sc)
	}
	return sl, nil
}

func (s *shopping_cart) del(db *pgx.Conn, table, field, value string) error {
	exec := fmt.Sprintf("delete from %v where %v=$1", table, field)
	_, err := db.Exec(context.Background(), exec, value)
	return err
}

type orderData struct {
	SessionID       string `json:"sessionid"`
	OrderDate       string `json:"orderdate"`
	ShoppingOrderID string `json:"shoppingorderid"`
	PricingID       string `json:"pricingid"`
	ShoppingCartID  string `json:"shoppingcartid"`
	Qty             string `json:"qty"`
	ParticipantID   string `json:"participantid"`
	ParticipantName string `json:"participantname"`
	OptionName      string `json:"optionname"`
	Category        string `json:"category"`
}

func (c *orderData) read(db *pgx.Conn, sessionID string) ([]orderData, error) {
	var odl []orderData
	var od orderData
	var orderdate time.Time
	var shoppingorderid int
	var shoppingcartid int
	var participantid int
	var qty int
	rows, err := db.Query(context.Background(), `
	select o.sessionID,  o.orderdate, o.shoppingorderID,
    c.pricingID, c.shoppingcartID, c.qty, p.cartparticipantid as ParticipantID, p.name as ParticipantName, oi.name as OptionName, co.name as Category
from shopping_order o
inner join shopping_cart c on
    o.shoppingorderID = c.shoppingorderID
inner join cart_participant p on
    p.shoppingcartID = c.shoppingcartID
inner join cart_participant_option po on 
    po.cartparticipantID = p.cartparticipantID
inner join  option_item oi on
    oi.optionitemsID = po.optionitemsID
inner join category_option co on 
    co.categoryoptionsID = oi.categoryoptionsID
		where o.sessionID = $1
	`, sessionID)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		err := rows.Scan(&od.SessionID, &orderdate, &shoppingorderid, &od.PricingID, &shoppingcartid, &qty, &participantid, &od.ParticipantName, &od.OptionName, &od.Category)
		if err != nil {
			return nil, err
		}
		od.OrderDate = orderdate.String()
		od.ShoppingOrderID = strconv.Itoa(shoppingorderid)
		od.ShoppingCartID = strconv.Itoa(shoppingcartid)
		od.ParticipantID = strconv.Itoa(participantid)
		od.Qty = strconv.Itoa(qty)
		odl = append(odl, od)
	}
	return odl, nil
}

type cart_participant_option struct {
	CartParticipantOptionID string `json:"cartparticipantoptionid"`
	CartParticipantID       string `json:"cartparticipantid"`
	OptionItemsID           string `json:"optionitemsid"`
}

func updateCartParticipantOption(db *pgx.Conn, d Data, i cart_participant_option) error {

	// convert struct to map
	imap := make(map[string]string)
	m, err := json.Marshal(i)
	if err != nil {
		return err
	}
	json.Unmarshal(m, &imap)

	exec, err := updateStringBuilder(d.Table, d.Update.SetFields, d.Update.SetValues, imap)
	_, err = db.Exec(context.Background(), exec)
	return err
}

func (c cart_participant_option) create(db *pgx.Conn, table string) error {
	exec := fmt.Sprintf("insert into %v(cartparticipantid, optionitemsid) values($1, $2)", table)
	_, err := db.Exec(context.Background(), exec, c.CartParticipantID, c.OptionItemsID)
	return err
}

func (c *cart_participant_option) readall(db *pgx.Conn, table string) ([]cart_participant_option, error) {
	var cpo cart_participant_option
	var cl []cart_participant_option
	query := fmt.Sprintf("select * from %v", table)
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&cpo.CartParticipantID, &cpo.OptionItemsID)
		if err != nil {
			return nil, err
		}
		cl = append(cl, cpo)
	}
	return cl, nil
}

func (c *cart_participant_option) read(db *pgx.Conn, table, field, value string) ([]cart_participant_option, error) {
	var cpo cart_participant_option
	var cl []cart_participant_option
	var cpoid int
	var cpid int
	var oiid int
	query := fmt.Sprintf("select * from %v where %v=$1", table, field)
	rows, err := db.Query(context.Background(), query, value)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&cpoid, &cpid, &oiid)
		if err != nil {
			return nil, err
		}
		cpo.CartParticipantOptionID = strconv.Itoa(cpoid)
		cpo.CartParticipantID = strconv.Itoa(cpid)
		cpo.OptionItemsID = strconv.Itoa(oiid)
		cl = append(cl, cpo)
	}
	return cl, nil
}

func (c *cart_participant_option) del(db *pgx.Conn, table, field, value string) error {
	exec := fmt.Sprintf("delete from %v where %v=$1", table, field)
	_, err := db.Exec(context.Background(), exec, value)
	return err
}

// Data used to unmarshal json in request to handler func
type Data struct {
	Action string          `json:"action"`
	Table  string          `json:"table"`
	Create json.RawMessage `json:"create"`
	Read   struct {
		Field string `json:"field"`
		Value string `json:"value"`
	} `json:"read"`
	Update struct {
		Identifiers json.RawMessage `json:"identifiers"`
		SetFields   []string        `json:"setfields"`
		SetValues   []string        `json:"setvalues"`
	} `json:"update"`
	Delete struct {
		Field  string   `json:"field"`
		Value  string   `json:"value"`
		Values []string `json:"values"`
	} `json:"delete"`
}

func dbConnect(user, pass, addr, name string) (*pgx.Conn, error) {
	databaseURL := fmt.Sprintf("postgres://%v:%v@%v:5432/%v", user, pass, addr, name)
	conn, err := pgx.Connect(context.Background(), databaseURL)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func getSecrets() (user, pass, addr, name []byte, err error) {
	var vd vaultutils.VaultData
	vd.Action = "getSecret"
	vd.Path = "db/mojodomodb"

	vd.Key = "user"
	user, err = httputils.PostRequest(vd, "http://10.62.0.1:8080/function/apikeycontroller", nil)
	if err != nil {
		return
	}
	vd.Key = "pass"
	pass, err = httputils.PostRequest(vd, "http://10.62.0.1:8080/function/apikeycontroller", nil)
	if err != nil {
		return
	}
	vd.Key = "address"
	addr, err = httputils.PostRequest(vd, "http://10.62.0.1:8080/function/apikeycontroller", nil)
	if err != nil {
		return
	}
	vd.Key = "database"
	name, err = httputils.PostRequest(vd, "http://10.62.0.1:8080/function/apikeycontroller", nil)
	if err != nil {
		return
	}
	return user, pass, addr, name, nil
}

func errResponse(err error) (handler.Response, error) {
	return handler.Response{
		Body:       []byte(err.Error()),
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"Access-Control-Allow-Origin":  {"*"},
			"Access-Control-Allow-Methods": {"*"},
			"Access-Control-Allow-Headers": {"*"},
			"Content-Type":                 {"application/json"},
		},
	}, err
}

func structResponse(i interface{}) (handler.Response, error) {
	resp, err := json.Marshal(i)
	return handler.Response{
		Body:       resp,
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"Access-Control-Allow-Origin":  {"*"},
			"Access-Control-Allow-Methods": {"*"},
			"Access-Control-Allow-Headers": {"*"},
			"Content-Type":                 {"application/json"},
		},
	}, err
}

func stringResponse(s string) (handler.Response, error) {
	return handler.Response{
		Body:       []byte(s),
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"Access-Control-Allow-Origin":  {"*"},
			"Access-Control-Allow-Methods": {"*"},
			"Access-Control-Allow-Headers": {"*"},
			"Content-Type":                 {"application/json"},
		},
	}, nil
}

func createResponse(db *pgx.Conn, d Data, c Creater) (handler.Response, error) {
	err := json.Unmarshal(d.Create, &c)
	if err != nil {
		return errResponse(err)
	}
	err = c.create(db, d.Table)
	if err != nil {
		return errResponse(err)
	}
	return stringResponse("success!")
}

// UpdateStringBuilder .
func updateStringBuilder(table string, sf, sv []string, m map[string]string) (string, error) {
	if len(sf) != len(sv) {
		return "", errors.New("updatestringbuilder err: string slice sf and v are not the same length")
	}
	updateString := fmt.Sprintf("update %v set %v='%v'", table, sf[0], sv[0])

	for i := 1; i < len(sf); i++ {
		updateString += fmt.Sprintf(",%v=%v", sf[i], sv[i])
	}

	first := true
	for k, v := range m {
		if v != "" {
			if first {
				updateString += fmt.Sprintf(" where %v='%v'", k, v)
				first = false
			} else {
				updateString += fmt.Sprintf(" and %v='%v'", k, v)
			}
		}
	}

	return updateString, nil
}

func deleteItemsFromShoppingCart(db *pgx.Conn, shoppingcartids []string) error {
	var err error

	for _, s := range shoppingcartids {
		shoppingcartid, err := strconv.Atoi(s)
		if err != nil {
			return err
		}

		//Deletes all shopping cart entries per shoppingcartid
		exec := fmt.Sprintf(`
		delete from cart_participant_option
		where cartParticipantId in (
		select cartParticipantId from cart_participant
		where shoppingcartid = $1
		)
		`)
		_, err = db.Exec(context.Background(), exec, shoppingcartid)
		if err != nil {
			return fmt.Errorf("cart_participant_option: %v", err.Error())
		}

		exec = fmt.Sprintf(`
		delete from  cart_participant 
		where shoppingcartid = $1
		`)
		_, err = db.Exec(context.Background(), exec, shoppingcartid)
		if err != nil {
			return fmt.Errorf("cart_participant: %v", err.Error())
		}

		exec = fmt.Sprintf(`
		delete from  shopping_cart 
		where shoppingcartid = $1
		`)
		_, err = db.Exec(context.Background(), exec, shoppingcartid)
		if err != nil {
			return fmt.Errorf("shopping_cart: %v", err.Error())
		}

	}

	return err
}

func deleteShoppingCart(db *pgx.Conn, shoppingcartid string) error {
	//Deletes all shopping cart entries per shoppingcartid
	exec := fmt.Sprintf(`
	delete from cart_participant_option
	where cartParticipantId in (
	select cartParticipantId from cart_participant
	where shoppingcartid = $1
	)
	`)
	_, err := db.Exec(context.Background(), exec, shoppingcartid)
	if err != nil {
		return fmt.Errorf("cart_participant_option: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	delete from  cart_participant 
	where shoppingcartid = $1
	`)
	_, err = db.Exec(context.Background(), exec, shoppingcartid)
	if err != nil {
		return fmt.Errorf("cart_participant: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	delete from  shopping_cart 
	where shoppingcartid = $1
	`)
	_, err = db.Exec(context.Background(), exec, shoppingcartid)
	if err != nil {
		return fmt.Errorf("shopping_cart: %v", err.Error())
	}

	return err
}

type migrate_data struct {
	ShoppingOrderID string `json:"shoppingorderid"`
	CustomerID      string `json:"customerid"`
	PaymentID       string `json:"paymentid"`
	Name            string `json:"name"`
	Email           string `json:"email"`
	Phone           string `json:"phone"`
}

func migrateData(db *pgx.Conn, md migrate_data) error {
	var customerID int
	exec := fmt.Sprintf(`
	insert into
    customer (organizationid, name, email, phone)
		values ($1, $2, $3, $4) returning customerid`)
	err := db.QueryRow(context.Background(), exec, "aa9a52a7-ab83-46ff-ab15-b35bd868407f", md.Name, md.Email, md.Phone).Scan(&customerID)
	if err != nil {
		return fmt.Errorf("Customer: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	insert into
    salesorder (salesorderid, customerid, orderdate, paymentid, invoiceno)
	select
    so.shoppingorderid,
    $1,
    so.orderdate,
    $2,
		$3
	from
    shopping_order so
	where
    so.shoppingorderid = $4`)
	_, err = db.Exec(context.Background(), exec, customerID, md.PaymentID, "none", md.ShoppingOrderID)
	if err != nil {
		return fmt.Errorf("salesorder: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	insert into
					purchase (
					purchaseid,
					salesorderid,
					qty,
					productname,
					description,
					price
			)
	select
			sc.shoppingcartid,
			so.shoppingorderid,
			sc.qty,
			pr.description,
			pr.description,
			p.price
	from
			shopping_cart sc
			inner join pricing p on sc.pricingid = p.pricingid
			inner join product pr on p.productid = pr.productid
			inner join shopping_order so on sc.shoppingorderid = so.shoppingorderid
	and so.shoppingorderid = $1`)
	_, err = db.Exec(context.Background(), exec, md.ShoppingOrderID)
	if err != nil {
		return fmt.Errorf("purchase: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	insert into
			participant (participantid, purchaseid, name)
	select
			cp.cartparticipantid,
			sc.shoppingcartid,
			cp.name
	from
			cart_participant cp
			inner join shopping_cart sc on cp.shoppingcartid = sc.shoppingcartid
			inner join shopping_order so on so.shoppingorderid = sc.shoppingorderid
	where
			so.shoppingorderid = $1
	`)
	_, err = db.Exec(context.Background(), exec, md.ShoppingOrderID)
	if err != nil {
		return fmt.Errorf("participant: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	insert into
			participant_option (
					participantoptionsid,
					participantid,
					optionitemsid
			)
	select
			cpo.cartparticipantoptionsid,
			cpo.cartparticipantid,
			cpo.optionitemsid
	from
			cart_participant_option cpo
			inner join cart_participant cp on cp.cartparticipantid = cpo.cartparticipantid
			inner join shopping_cart sc on cp.shoppingcartid = sc.shoppingcartid
			inner join shopping_order so on sc.shoppingorderid = so.shoppingorderid
	where
			so.shoppingorderid = $1
	`)
	_, err = db.Exec(context.Background(), exec, md.ShoppingOrderID)
	if err != nil {
		return fmt.Errorf("participant_option: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	delete from cart_participant_option 
	where cartparticipantid in (
			select cartparticipantid 
			from cart_participant cp
			inner join shopping_cart sc on sc.shoppingcartid = cp.shoppingcartid
			where sc.shoppingorderid = $1)`)
	_, err = db.Exec(context.Background(), exec, md.ShoppingOrderID)
	if err != nil {
		return fmt.Errorf("deleting cart_participant_option: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	delete from cart_participant
	where shoppingcartid in (
			select shoppingcartid 
			from shopping_cart
			where shoppingorderid = $1)`)
	_, err = db.Exec(context.Background(), exec, md.ShoppingOrderID)
	if err != nil {
		return fmt.Errorf("deleting cart_participant: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	delete from shopping_cart 
	where shoppingorderid = $1`)
	_, err = db.Exec(context.Background(), exec, md.ShoppingOrderID)
	if err != nil {
		return fmt.Errorf("deleting shopping_cart: %v", err.Error())
	}

	exec = fmt.Sprintf(`
	delete from shopping_order
	where shoppingorderid = $1
	`)
	_, err = db.Exec(context.Background(), exec, md.ShoppingOrderID)
	if err != nil {
		return fmt.Errorf("deleting orderid: %v", err.Error())
	}

	return nil
}

// Handle a function invocation
func Handle(req handler.Request) (handler.Response, error) {
	// validate request api key
	err := vaultutils.Auth(req, "db", "http://10.62.0.1:8080/function/apikeycontroller")
	if err != nil {
		errResponse(err)
	}

	// get databases info for connection
	user, pass, addr, name, err := getSecrets()
	if err != nil {
		errResponse(err)
	}

	// connect to database
	db, err := dbConnect(string(user), string(pass), string(addr), string(name))
	if err != nil {
		errResponse(err)
	}

	defer db.Close(context.Background())

	var d Data
	err = json.Unmarshal(req.Body, &d)
	if err != nil {
		errResponse(err)
	}

	// CREATE
	switch {
	case strings.ToLower(d.Action) == "create":
		switch {
		case strings.ToLower(d.Table) == "salesorder":
			var o salesorder
			err := json.Unmarshal(d.Create, &o)
			if err != nil {
				return errResponse(err)
			}
			err = o.create(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		case strings.ToLower(d.Table) == "participant":
			var p participant
			err := json.Unmarshal(d.Create, &p)
			if err != nil {
				return errResponse(err)
			}
			err = p.create(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		case strings.ToLower(d.Table) == "shopping_cart":
			var s shopping_cart
			err := json.Unmarshal(d.Create, &s)
			if err != nil {
				return errResponse(err)
			}
			shoppingcartid, err := s.create(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse(strconv.Itoa(shoppingcartid))
			//............................................

		case strings.ToLower(d.Table) == "shopping_order":
			var s shopping_order
			err := json.Unmarshal(d.Create, &s)
			if err != nil {
				return errResponse(err)
			}
			shoppingorderid, err := s.create(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse(strconv.Itoa(shoppingorderid))
			//............................................

		case strings.ToLower(d.Table) == "cart_participant":
			var c cart_participant
			err := json.Unmarshal(d.Create, &c)
			if err != nil {
				return errResponse(err)
			}
			cartparticipantid, err := c.create(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse(strconv.Itoa(cartparticipantid))
			//............................................

		case strings.ToLower(d.Table) == "registration":
			var r registration
			err := json.Unmarshal(d.Create, &r)
			if err != nil {
				return errResponse(err)
			}

			err = r.create(db)
			if err != nil {
				return errResponse(err)
			}

			return stringResponse("success!")
			//............................................

		case strings.ToLower(d.Table) == "cart_participant_option":
			var c cart_participant_option
			err := json.Unmarshal(d.Create, &c)
			if err != nil {
				return errResponse(err)
			}
			err = c.create(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		case strings.ToLower(d.Table) == "migrate_data":
			var m migrate_data
			err := json.Unmarshal(d.Create, &m)
			if err != nil {
				return errResponse(err)
			}
			err = migrateData(db, m)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		}

	// READ
	// Reads in single row, field is the tables column to query by and value is
	// the value of that field
	// ex.___________________
	// select * from <table_name> where <field> = <value>
	case strings.ToLower(d.Action) == "read":

		switch {
		case strings.ToLower(d.Table) == "order":
			var o salesorder
			ol, err := o.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(ol)
			//............................................

		case strings.ToLower(d.Table) == "category_options":
			var c category_options
			cl, err := c.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(cl)
			//............................................

		case strings.ToLower(d.Table) == "customer":
			var c customer
			cl, err := c.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(cl)
			//............................................

		case strings.ToLower(d.Table) == "event":
			var e event
			el, err := e.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(el)
			//............................................

		case strings.ToLower(d.Table) == "option_items":
			var o option_items
			ol, err := o.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(ol)
			//............................................

		case strings.ToLower(d.Table) == "organization":
			var o organization
			ol, err := o.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(ol)
			//............................................

		case strings.ToLower(d.Table) == "participant":
			var p participant
			pl, err := p.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(pl)
			//............................................

		case strings.ToLower(d.Table) == "participant_options":
			var p participant_options
			pl, err := p.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(pl)
			//............................................

		case strings.ToLower(d.Table) == "purchase":
			var p purchase
			pl, err := p.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(pl)
			//............................................

		case strings.ToLower(d.Table) == "shopping_order":
			var s shopping_order
			sl, err := s.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(sl)
			//............................................

		case strings.ToLower(d.Table) == "shopping_cart":
			var s shopping_cart
			sl, err := s.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(sl)
			//............................................

		case strings.ToLower(d.Table) == "cart_participant":
			var c cart_participant
			cl, err := c.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(cl)
			//............................................

		case strings.ToLower(d.Table) == "cart_participant_option":
			var c cart_participant_option
			cl, err := c.read(db, d.Table, d.Read.Field, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(cl)
			//............................................

		case strings.ToLower(d.Table) == "order_data":
			var o orderData
			ol, err := o.read(db, d.Read.Value)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(ol)
			//............................................

		case strings.ToLower(d.Table) == "dashboard_summary":
			var ds dashboardSummary
			err := ds.getDashboardSummary(db)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(ds)
			//............................................

		case strings.ToLower(d.Table) == "registration_summary":
			var rs registrationSummary
			err := rs.getRegistrationSummary(db)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(rs)
			//............................................

		case strings.ToLower(d.Table) == "shirt_summary":
			var ss shirtSummary
			err := ss.getShirtSummary(db)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(ss)
			//............................................

		case strings.ToLower(d.Table) == "club_summary":
			var cs clubSummary
			err := cs.getClubSummary(db)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(cs)
			//............................................

		case strings.ToLower(d.Table) == "registration_breakdown":
			var rb registrationBreakdown
			err := rb.getRegistrationBreakdown(db)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(rb)
			//............................................

		case strings.ToLower(d.Table) == "registration_detail":
			rdList, err := getRegistrationDetail(db)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(rdList)
			//............................................
		}

	// READALL
	case strings.ToLower(d.Action) == "readall":
		switch {
		case strings.ToLower(d.Table) == "order":
			var o salesorder
			ol, err := o.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(ol)
			//............................................

		case strings.ToLower(d.Table) == "category_options":
			var c category_options
			cl, err := c.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(cl)
			//............................................

		case strings.ToLower(d.Table) == "customer":
			var c customer
			cl, err := c.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(cl)
			//............................................

		case strings.ToLower(d.Table) == "event":
			var e event
			el, err := e.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(el)
			//............................................

		case strings.ToLower(d.Table) == "option_items":
			var o option_items
			ol, err := o.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(ol)
			//............................................

		case strings.ToLower(d.Table) == "organization":
			var o organization
			oa, err := o.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(oa)
			//............................................

		case strings.ToLower(d.Table) == "participant":
			var p participant
			pl, err := p.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(pl)
			//............................................

		case strings.ToLower(d.Table) == "participant_options":
			var p participant_options
			pl, err := p.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(pl)
			//............................................

		case strings.ToLower(d.Table) == "purchase":
			var p purchase
			pl, err := p.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(pl)
			//............................................

		case strings.ToLower(d.Table) == "shopping_order":
			var s shopping_order
			sl, err := s.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(sl)
			//............................................

		case strings.ToLower(d.Table) == "shopping_cart":
			var s shopping_cart
			sl, err := s.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(sl)
			//............................................

		case strings.ToLower(d.Table) == "cart_participant":
			var c cart_participant
			sl, err := c.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(sl)
			//............................................

		case strings.ToLower(d.Table) == "cart_participant_option":
			var c cart_participant_option
			sl, err := c.readall(db, d.Table)
			if err != nil {
				return errResponse(err)
			}
			return structResponse(sl)
			//............................................

		}

	// UPDATE
	case strings.ToLower(d.Action) == "update":
		switch {
		case strings.ToLower(d.Table) == "salesorder":
			var si salesorder
			err := json.Unmarshal(d.Update.Identifiers, &si)
			if err != nil {
				return errResponse(err)
			}
			err = updateSalesOrder(db, d, si)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		case strings.ToLower(d.Table) == "category_options":
			//............................................

		case strings.ToLower(d.Table) == "customer":
			//............................................

		case strings.ToLower(d.Table) == "event":
			//............................................

		case strings.ToLower(d.Table) == "option_items":
			//............................................

		case strings.ToLower(d.Table) == "organization":
			//............................................

		case strings.ToLower(d.Table) == "package_category":
			//............................................

		case strings.ToLower(d.Table) == "participant":
			//............................................

		case strings.ToLower(d.Table) == "participant_options":
			//............................................

		case strings.ToLower(d.Table) == "purchase":
			//............................................

		case strings.ToLower(d.Table) == "shopping_cart":
			var si shopping_cart
			err := json.Unmarshal(d.Update.Identifiers, &si)
			if err != nil {
				return errResponse(err)
			}
			err = updateShoppingCart(db, d, si)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		case strings.ToLower(d.Table) == "shopping_order":
			var si shopping_order
			err := json.Unmarshal(d.Update.Identifiers, &si)
			if err != nil {
				return errResponse(err)
			}
			err = updateShoppingOrder(db, d, si)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		case strings.ToLower(d.Table) == "cart_participant":
			var ci cart_participant
			err := json.Unmarshal(d.Update.Identifiers, &ci)
			if err != nil {
				return errResponse(err)
			}
			err = updateCartParticipant(db, d, ci)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		case strings.ToLower(d.Table) == "cart_participant_option":
			var ci cart_participant_option
			err := json.Unmarshal(d.Update.Identifiers, &ci)
			if err != nil {
				return errResponse(err)
			}
			err = updateCartParticipantOption(db, d, ci)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//............................................

		}

	// DELETE
	case strings.ToLower(d.Action) == "delete":
		switch {

		case strings.ToLower(d.Table) == "shopping_cart":
			shoppingCartID := d.Delete.Value
			if err != nil {
				return errResponse(err)
			}
			err = deleteShoppingCart(db, shoppingCartID)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//...........................................

		/// TODO: test
		case strings.ToLower(d.Table) == "shopping_carts":
			shoppingCartID := d.Delete.Values
			if err != nil {
				return errResponse(err)
			}
			err = deleteItemsFromShoppingCart(db, shoppingCartID)
			if err != nil {
				return errResponse(err)
			}
			return stringResponse("success!")
			//...........................................
		}
	}

	return errResponse(errors.New("error: could not retrieve data: " + err.Error()))
}
