// Auto-generated , DO NOT EDIT
import {Entity, FunctionPropertyNames, FieldsExpression, GetOptions } from "@subql/types-core";
import assert from 'assert';



export type AccountProps = Omit<Account, NonNullable<FunctionPropertyNames<Account>>| '_name'>;

export class Account implements Entity {

    constructor(
        
        id: string,
        publicKey: string,
    ) {
        this.id = id;
        this.publicKey = publicKey;
        
    }

    public id: string;
    public publicKey: string;
    public firstChangeBlock?: number;
    public lastChangeBlock?: number;
    

    get _name(): string {
        return 'Account';
    }

    async save(): Promise<void>{
        let id = this.id;
        assert(id !== null, "Cannot save Account entity without an ID");
        await store.set('Account', id.toString(), this);
    }

    static async remove(id:string): Promise<void>{
        assert(id !== null, "Cannot remove Account entity without an ID");
        await store.remove('Account', id.toString());
    }

    static async get(id:string): Promise<Account | undefined>{
        assert((id !== null && id !== undefined), "Cannot get Account entity without an ID");
        const record = await store.get('Account', id.toString());
        if (record) {
            return this.create(record as AccountProps);
        } else {
            return;
        }
    }


    /**
     * Gets entities matching the specified filters and options.
     *
     * ⚠️ This function will first search cache data followed by DB data. Please consider this when using order and offset options.⚠️
     * */
    static async getByFields(filter: FieldsExpression<AccountProps>[], options?: GetOptions<AccountProps>): Promise<Account[]> {
        const records = await store.getByFields('Account', filter, options);
        return records.map(record => this.create(record as AccountProps));
    }

    static create(record: AccountProps): Account {
        assert(typeof record.id === 'string', "id must be provided");
        let entity = new this(
            record.id,
            record.publicKey,
        );
        Object.assign(entity,record);
        return entity;
    }
}
